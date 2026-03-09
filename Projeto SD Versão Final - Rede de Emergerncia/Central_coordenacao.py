import socket
import threading
import json
import time
import stomp
from config_rede import IP_SERVIDOR_NOMES, PORTA_SERVIDOR_NOMES, PORTA_CENTRAL

#CONFIGURAÇÕES DO PROCESSO
PROCESS_ID = 1
NUM_PROCESSOS = 8

lamport_clock = 0
vector_clock = [0] * NUM_PROCESSOS

NOME_ENTIDADE = "CENTRAL_COORDENACAO"

#CONFIGURAÇÕES DO ACTIVEMQ
BROKER_IP = "localhost"
BROKER_PORT = 61613

FILA_INCIDENTES_VALIDADOS = "/queue/incidentes.validados"
FILA_DESPACHO = {
    "AMBULANCIA_01": "/queue/despacho.ambulancia",
    "BOMBEIROS_01": "/queue/despacho.bombeiros",
    "VIATURA_POLICIAL_01": "/queue/despacho.viatura",
    "UNIDADE_APOIO_01": "/queue/despacho.apoio"
}

#CONTROLE DE LIDERANÇA
lider_atual = NOME_ENTIDADE

#BANCO DE OCORRÊNCIAS
banco_ocorrencias = []

#CONTROLE DE INCIDENTES
fila_incidentes = []
incidentes_em_andamento = {}

#CONTROLE DE SEÇÃO CRÍTICA
fila_sc = []
processo_em_sc = None

#CONTROLE DE CONEXÕES
conexoes_ativas = {}
lock_conexoes = threading.Lock()
lock_incidentes = threading.Lock()
lock_sc = threading.Lock()

#MAPEAMENTO DE INCIDENTES
MAPA_ATENDIMENTO = {
    "INCENDIO": ["BOMBEIROS_01", "UNIDADE_APOIO_01", "VIATURA_POLICIAL_01", "AMBULANCIA_01"],
    "ACIDENTE_VEICULO": ["AMBULANCIA_01", "VIATURA_POLICIAL_01"],
    "ASSALTO": ["VIATURA_POLICIAL_01"],
    "DESABAMENTO": ["BOMBEIROS_01", "UNIDADE_APOIO_01", "AMBULANCIA_01", "VIATURA_POLICIAL_01"],
    "EMERGENCIA_MEDICA": ["AMBULANCIA_01"]
}

#CONEXÃO GLOBAL COM BROKER
conn_broker = None

#FUNÇÕES VISUAIS
def separador_simples():
    print("\n" + "-" * 80)


def separador_destaque():
    print("\n" + "=" * 80)


def titulo_bloco(titulo):
    print("\n" + "=" * 80)
    print(titulo)
    print("=" * 80)


#RELÓGIOS LÓGICOS
def incrementar_relogio_local():
    global lamport_clock, vector_clock
    lamport_clock += 1
    vector_clock[PROCESS_ID] += 1


def atualizar_relogio_ao_receber(lamport_recebido, vetor_recebido):
    global lamport_clock, vector_clock

    lamport_clock = max(lamport_clock, lamport_recebido) + 1

    if isinstance(vetor_recebido, list) and len(vetor_recebido) == NUM_PROCESSOS:
        for i in range(NUM_PROCESSOS):
            vector_clock[i] = max(vector_clock[i], vetor_recebido[i])

    vector_clock[PROCESS_ID] += 1


#REGISTRO NO SERVIDOR DE NOMES
def registrar_no_servidor_nomes():
    IP_NS, PORTA_NS = IP_SERVIDOR_NOMES, PORTA_SERVIDOR_NOMES
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    incrementar_relogio_local()

    dados = {
        "tipo_requisicao": "REGISTRAR",
        "nome": NOME_ENTIDADE,
        "porta": PORTA_CENTRAL,
        "servico": "COORDINATION",
        "lamport": lamport_clock,
        "vetor": list(vector_clock)
    }

    try:
        sock.sendto(json.dumps(dados).encode("utf-8"), (IP_NS, PORTA_NS))
        data, _ = sock.recvfrom(2048)
        resposta = json.loads(data.decode("utf-8"))

        atualizar_relogio_ao_receber(
            resposta.get("lamport", 0),
            resposta.get("vetor", [0] * NUM_PROCESSOS)
        )

        separador_simples()
        print(f"[CENTRAL] Registro OK no Servidor de Nomes | Lamport={lamport_clock}")

    except Exception as e:
        print(f"[ERRO] Falha ao registrar no Servidor de Nomes: {e}")

    finally:
        sock.close()


#BANCO DE OCORRÊNCIAS
def registrar_ocorrencia(evento, origem, id_incidente=None):
    global banco_ocorrencias

    incrementar_relogio_local()

    registro = {
        "origem": origem,
        "evento": evento,
        "id_incidente": id_incidente,
        "timestamp_lamport": lamport_clock,
        "timestamp_vetorial": list(vector_clock),
        "timestamp_fisico": time.ctime()
    }

    banco_ocorrencias.append(registro)

    separador_simples()
    print(
        f"[BANCO] Registro salvo | "
        f"Origem={origem} | "
        f"Incidente={id_incidente} | "
        f"Evento={evento} | "
        f"Lamport={lamport_clock}"
    )


#ENVIO SEGURO TCP
def enviar_seguro(conn, mensagem):
    try:
        conn.send((json.dumps(mensagem) + "\n").encode("utf-8"))
        return True
    except Exception:
        return False


def broadcast_lider():
    incrementar_relogio_local()

    msg = {
        "tipo_mensagem": "LIDER_ELEITO",
        "origem": NOME_ENTIDADE,
        "lider_atual": lider_atual,
        "conteudo": f"Líder atual definido: {lider_atual}",
        "lamport": lamport_clock,
        "vetor": list(vector_clock)
    }

    with lock_conexoes:
        for _, conn in list(conexoes_ativas.items()):
            enviar_seguro(conn, msg)


#ACTIVEMQ 
def conectar_broker():
    global conn_broker
    try:
        conn_broker = stomp.Connection([(BROKER_IP, BROKER_PORT)])
        conn_broker.connect(wait=True)
        separador_simples()
        print(f"[CENTRAL] Conectada ao ActiveMQ em {BROKER_IP}:{BROKER_PORT}")
        return True
    except Exception as e:
        print(f"[ERRO] Falha ao conectar ao ActiveMQ: {e}")
        return False


def publicar_no_broker(destino, mensagem):
    global conn_broker
    try:
        conn_broker.send(destination=destino, body=json.dumps(mensagem))
        return True
    except Exception as e:
        print(f"[ERRO] Falha ao publicar no ActiveMQ ({destino}): {e}")
        return False


class ListenerCentral(stomp.ConnectionListener):
    def on_error(self, frame):
        print(f"[CENTRAL][BROKER][ERRO] {frame.body}")

    def on_message(self, frame):
        try:
            msg_recebida = json.loads(frame.body)
        except json.JSONDecodeError:
            separador_simples()
            print("[CENTRAL] Mensagem inválida recebida do broker, ignorando...")
            return

        atualizar_relogio_ao_receber(
            msg_recebida.get("lamport", 0),
            msg_recebida.get("vetor", [0] * NUM_PROCESSOS)
        )

        tipo = msg_recebida.get("tipo_mensagem")
        origem = msg_recebida.get("origem", "DESCONHECIDO")

        if tipo == "INCIDENTE_MONITORADO":
            conteudo = msg_recebida.get("conteudo", {})

            titulo_bloco(
                f"[INCIDENTE RECEBIDO VIA ACTIVEMQ] "
                f"ID={conteudo.get('id_incidente')} | "
                f"TIPO={conteudo.get('tipo_incidente')} | "
                f"Origem={origem} | Lamport={lamport_clock}"
            )

            adicionar_incidente_fila(msg_recebida)

        else:
            separador_simples()
            print(f"[CENTRAL] Tipo de mensagem não tratado no broker: {tipo}")


#ELEIÇÃO DE LÍDER
def tratar_eleicao(msg_recebida, conn):
    global lider_atual

    origem = msg_recebida.get("origem", "DESCONHECIDO")
    process_id_remoto = msg_recebida.get("process_id", -1)

    incrementar_relogio_local()

    lider_atual = NOME_ENTIDADE

    resposta = {
        "tipo_mensagem": "LIDER_ELEITO",
        "origem": NOME_ENTIDADE,
        "lider_atual": lider_atual,
        "conteudo": f"Eleição concluída. {lider_atual} permanece como líder.",
        "lamport": lamport_clock,
        "vetor": list(vector_clock)
    }

    enviar_seguro(conn, resposta)

    titulo_bloco(
        f"[ELEIÇÃO] Solicitação recebida de {origem} (PID={process_id_remoto}) | "
        f"Líder mantido: {lider_atual}"
    )

    broadcast_lider()


#INCIDENTES
def adicionar_incidente_fila(msg_recebida):
    conteudo = msg_recebida.get("conteudo", {})
    id_incidente = conteudo.get("id_incidente")
    tipo_incidente = conteudo.get("tipo_incidente")

    with lock_incidentes:
        fila_incidentes.append(conteudo)

    separador_destaque()
    print(
        f"[CENTRAL] Incidente enfileirado -> "
        f"ID={id_incidente} | "
        f"TIPO={tipo_incidente}"
    )
    print(f"[CENTRAL] Total na fila: {len(fila_incidentes)}")
    print("=" * 80)


def despachar_unidades(incidente):
    tipo_incidente = incidente.get("tipo_incidente")
    id_incidente = incidente.get("id_incidente")
    unidades_necessarias = MAPA_ATENDIMENTO.get(tipo_incidente, [])

    if not unidades_necessarias:
        separador_simples()
        print(f"[CENTRAL] Nenhuma unidade mapeada para o incidente {tipo_incidente}")
        return

    titulo_bloco(f"[CENTRAL] DESPACHANDO UNIDADES VIA ACTIVEMQ -> {id_incidente} ({tipo_incidente})")

    for unidade in unidades_necessarias:
        incrementar_relogio_local()

        msg_comando = {
            "tipo_mensagem": "COMANDO",
            "origem": NOME_ENTIDADE,
            "conteudo": f"ATENDER_INCIDENTE:{id_incidente}",
            "acao": "ATENDER_INCIDENTE",
            "id_incidente": id_incidente,
            "tipo_incidente": tipo_incidente,
            "detalhes_incidente": incidente,
            "lider_atual": lider_atual,
            "lamport": lamport_clock,
            "vetor": list(vector_clock)
        }

        fila_destino = FILA_DESPACHO.get(unidade)

        separador_simples()
        if fila_destino and publicar_no_broker(fila_destino, msg_comando):
            print(f"[DESPACHO] Comando publicado para {unidade} em {fila_destino} | Incidente={id_incidente}")
        else:
            print(f"[DESPACHO] Não foi possível publicar comando para {unidade} | Incidente={id_incidente}")


def processar_fila_incidentes():
    while True:
        incidente = None

        with lock_incidentes:
            if fila_incidentes:
                incidente = fila_incidentes.pop(0)

        if incidente:
            id_incidente = incidente.get("id_incidente")
            tipo_incidente = incidente.get("tipo_incidente")

            incrementar_relogio_local()

            incidentes_em_andamento[id_incidente] = {
                "tipo_incidente": tipo_incidente,
                "status": "EM_ATENDIMENTO",
                "detalhes": incidente,
                "timestamp_inicio": time.ctime(),
                "lamport_inicio": lamport_clock
            }

            titulo_bloco(
                f"[CENTRAL] PROCESSANDO INCIDENTE -> "
                f"ID={id_incidente} | TIPO={tipo_incidente} | Lamport={lamport_clock}"
            )

            registrar_ocorrencia(
                evento=f"Incidente {id_incidente} recebido e encaminhado para despacho.",
                origem=NOME_ENTIDADE,
                id_incidente=id_incidente
            )

            despachar_unidades(incidente)

        time.sleep(1)


#SEÇÃO CRÍTICA
def conceder_proximo_acesso_sc():
    global processo_em_sc

    if fila_sc and processo_em_sc is None:
        fila_sc.sort(key=lambda item: (item["lamport"], item["origem"]))

        proximo = fila_sc.pop(0)
        processo_em_sc = proximo["origem"]

        incrementar_relogio_local()

        grant_msg = {
            "tipo_mensagem": "GRANT_SC",
            "origem": NOME_ENTIDADE,
            "conteudo": "Acesso à seção crítica concedido.",
            "lider_atual": lider_atual,
            "lamport": lamport_clock,
            "vetor": list(vector_clock)
        }

        separador_simples()

        if enviar_seguro(proximo["conn"], grant_msg):
            print(f"[GRANT_SC] Acesso concedido para {processo_em_sc}")
        else:
            print(f"[ERRO] Falha ao enviar GRANT para {processo_em_sc}")
            processo_em_sc = None


#GERENCIAMENTO DE CLIENTES TCP
def registrar_conexao(origem, conn):
    with lock_conexoes:
        conexoes_ativas[origem] = conn

    separador_simples()
    print(f"[CENTRAL] Conexão associada à entidade: {origem}")


def remover_conexao(origem, conn):
    with lock_conexoes:
        if origem in conexoes_ativas and conexoes_ativas[origem] == conn:
            del conexoes_ativas[origem]

    separador_simples()
    print(f"[CENTRAL] Entidade desconectada: {origem}")


def tratar_mensagem(msg_recebida, conn, addr):
    global processo_em_sc, lider_atual

    atualizar_relogio_ao_receber(
        msg_recebida.get("lamport", 0),
        msg_recebida.get("vetor", [0] * NUM_PROCESSOS)
    )

    tipo = msg_recebida.get("tipo_mensagem")
    origem = msg_recebida.get("origem", f"{addr}")

    registrar_conexao(origem, conn)

    #EVENTO
    if tipo == "EVENTO":
        conteudo = msg_recebida.get("conteudo")

        separador_simples()
        print(f"[EVENTO RECEBIDO de {origem}] {conteudo} | Lamport={lamport_clock}")

        registrar_ocorrencia(
            evento=conteudo,
            origem=origem,
            id_incidente=msg_recebida.get("id_incidente")
        )

    #ELEIÇÃO
    elif tipo == "ELEICAO":
        tratar_eleicao(msg_recebida, conn)

    #REQUEST_SC
    elif tipo == "REQUEST_SC":
        with lock_sc:
            incremento_lamport_request = msg_recebida.get("lamport", lamport_clock)

            separador_simples()
            print(f"[REQUEST_SC] {origem} solicitou acesso ao banco | Lamport={lamport_clock}")

            fila_sc.append({
                "origem": origem,
                "conn": conn,
                "lamport": incremento_lamport_request
            })

            print(f"[FILA SC] Estado atual: {[item['origem'] for item in fila_sc]}")

            conceder_proximo_acesso_sc()

    #RELEASE_SC
    elif tipo == "RELEASE_SC":
        with lock_sc:
            separador_simples()
            print(f"[RELEASE_SC] {origem} liberou a seção crítica | Lamport={lamport_clock}")
            processo_em_sc = None
            conceder_proximo_acesso_sc()

    #REGISTRO DIRETO
    elif tipo == "REGISTRO_OCORRENCIA":
        conteudo = msg_recebida.get("conteudo")
        id_incidente = msg_recebida.get("id_incidente")
        registrar_ocorrencia(conteudo, origem, id_incidente)

    #LIDER_ELEITO
    elif tipo == "LIDER_ELEITO":
        lider_atual = msg_recebida.get("lider_atual", lider_atual)

        separador_simples()
        print(f"[CENTRAL] Confirmação de líder recebida: {lider_atual}")

    else:
        separador_simples()
        print(f"[AVISO] Tipo de mensagem desconhecido recebido de {origem}: {tipo}")


def gerenciar_cliente(conn, addr):
    origem_associada = None
    buffer = ""

    separador_simples()
    print(f"[NOVA CONEXÃO] {addr} conectado.")

    try:
        while True:
            data = conn.recv(4096)

            if not data:
                separador_simples()
                print(f"[DESCONECTADO] {addr} encerrou a conexão.")
                break

            buffer += data.decode("utf-8")

            while "\n" in buffer:
                linha, buffer = buffer.split("\n", 1)
                linha = linha.strip()

                if not linha:
                    continue

                try:
                    msg_recebida = json.loads(linha)
                except json.JSONDecodeError:
                    separador_simples()
                    print(f"[AVISO] Mensagem inválida de {addr}, ignorando...")
                    continue

                origem_associada = msg_recebida.get("origem", origem_associada)
                tratar_mensagem(msg_recebida, conn, addr)

    except Exception as e:
        separador_simples()
        print(f"[ERRO] Conexão com {addr} encerrada: {e}")

    finally:
        if origem_associada:
            remover_conexao(origem_associada, conn)
        conn.close()


#INICIALIZAÇÃO
def iniciar_central():
    registrar_no_servidor_nomes()

    if not conectar_broker():
        print("[ERRO] Central não conseguiu iniciar sem ActiveMQ.")
        return

    listener = ListenerCentral()
    conn_broker.set_listener("", listener)
    conn_broker.subscribe(
        destination=FILA_INCIDENTES_VALIDADOS,
        id=1,
        ack="auto"
    )

    thread_fila = threading.Thread(target=processar_fila_incidentes, daemon=True)
    thread_fila.start()

    IP = "0.0.0.0"
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    servidor.bind((IP, PORTA_CENTRAL))
    servidor.listen()

    titulo_bloco(f"[CENTRAL] Ouvindo na porta {PORTA_CENTRAL}... | Líder atual: {lider_atual}")
    print("[CENTRAL] Status: central ativa, coordenando atendimento e aguardando incidentes.")
    print(f"[CENTRAL] Consumindo incidentes validados em {FILA_INCIDENTES_VALIDADOS}")

    while True:
        conn, addr = servidor.accept()
        thread = threading.Thread(target=gerenciar_cliente, args=(conn, addr), daemon=True)
        thread.start()


if __name__ == "__main__":
    iniciar_central()