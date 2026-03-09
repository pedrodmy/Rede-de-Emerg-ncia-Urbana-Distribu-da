import socket
import json
import time
import threading
import stomp
from config_rede import IP_SERVIDOR_NOMES, PORTA_SERVIDOR_NOMES

#CONFIGURAÇÕES DO PROCESSO
PROCESS_ID = 5
NUM_PROCESSOS = 8

lamport_clock = 0
vector_clock = [0] * NUM_PROCESSOS

NOME_ENTIDADE = "UNIDADE_APOIO_01"
NOME_CENTRAL = "CENTRAL_COORDENACAO"

lider_atual = "DESCONHECIDO"

aguardando_grant = False
id_incidente_em_registro = None

#CONFIGURAÇÕES DO ACTIVEMQ
BROKER_IP = "localhost"
BROKER_PORT = 61613
FILA_DESPACHO = "/queue/despacho.apoio"
FILA_STATUS = "/queue/status.unidades"

# Controle de atendimento
fila_chamados = []
lock_fila = threading.Lock()

# RELÓGIOS LÓGICOS
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


# DESCOBERTA NO SERVIDOR DE NOMES
def consultar_servidor_nomes(nome_busca):
    IP_NS, PORTA_NS = IP_SERVIDOR_NOMES, PORTA_SERVIDOR_NOMES
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    incrementar_relogio_local()

    pergunta = {
        "tipo_requisicao": "CONSULTAR",
        "nome": nome_busca,
        "lamport": lamport_clock,
        "vetor": list(vector_clock)
    }

    try:
        sock.sendto(json.dumps(pergunta).encode("utf-8"), (IP_NS, PORTA_NS))
        data, _ = sock.recvfrom(2048)
        resposta = json.loads(data.decode("utf-8"))

        atualizar_relogio_ao_receber(
            resposta.get("lamport", 0),
            resposta.get("vetor", [0] * NUM_PROCESSOS)
        )

        return resposta

    except Exception as e:
        print(f"[ERRO] Falha ao consultar Servidor de Nomes: {e}")
        return None

    finally:
        sock.close()


#ENVIO SEGURO TCP
def enviar_seguro(cliente, mensagem):
    try:
        cliente.send((json.dumps(mensagem) + "\n").encode("utf-8"))
        return True
    except Exception as e:
        print(f"[ERRO] Falha ao enviar mensagem: {e}")
        return False


#ACTIVEMQ
def conectar_broker():
    try:
        conn = stomp.Connection([(BROKER_IP, BROKER_PORT)])
        conn.connect(wait=True)
        print(f"[APOIO] Conectado ao ActiveMQ em {BROKER_IP}:{BROKER_PORT}")
        return conn
    except Exception as e:
        print(f"[ERRO] Falha ao conectar ao ActiveMQ: {e}")
        return None


def publicar_status_broker(conn_broker, evento, id_incidente):
    try:
        incrementar_relogio_local()

        msg_status = {
            "tipo_mensagem": "STATUS_UNIDADE",
            "origem": NOME_ENTIDADE,
            "conteudo": evento,
            "id_incidente": id_incidente,
            "timestamp": time.ctime(),
            "lamport": lamport_clock,
            "vetor": list(vector_clock)
        }

        conn_broker.send(destination=FILA_STATUS, body=json.dumps(msg_status))
        print(f"[APOIO] STATUS publicado no broker | Incidente={id_incidente} | {evento}")
    except Exception as e:
        print(f"[ERRO] Falha ao publicar status no ActiveMQ: {e}")


class ListenerApoio(stomp.ConnectionListener):
    def on_error(self, frame):
        print(f"[APOIO][BROKER][ERRO] {frame.body}")

    def on_message(self, frame):
        global lider_atual

        try:
            msg = json.loads(frame.body)
        except json.JSONDecodeError:
            print("[APOIO] Mensagem inválida recebida do ActiveMQ, ignorando...")
            return

        atualizar_relogio_ao_receber(
            msg.get("lamport", 0),
            msg.get("vetor", [0] * NUM_PROCESSOS)
        )

        if msg.get("tipo_mensagem") == "COMANDO" and msg.get("acao") == "ATENDER_INCIDENTE":
            with lock_fila:
                fila_chamados.append(msg)

            lider_atual = msg.get("lider_atual", lider_atual)

            print(
                f"[APOIO] COMANDO recebido via ActiveMQ -> "
                f"Atender incidente {msg.get('id_incidente')} ({msg.get('tipo_incidente')})"
            )


#PROTOCOLO DE SEÇÃO CRÍTICA
def solicitar_secao_critica(cliente, id_incidente):
    global aguardando_grant, id_incidente_em_registro

    incrementar_relogio_local()
    aguardando_grant = True
    id_incidente_em_registro = id_incidente

    msg_request = {
        "tipo_mensagem": "REQUEST_SC",
        "origem": NOME_ENTIDADE,
        "id_incidente": id_incidente,
        "lamport": lamport_clock,
        "vetor": list(vector_clock)
    }

    enviar_seguro(cliente, msg_request)
    print(f"[SC] REQUEST_SC enviado | Incidente={id_incidente} | Lamport={lamport_clock}")


def registrar_no_banco(cliente, evento, id_incidente):
    incrementar_relogio_local()

    msg_registro = {
        "tipo_mensagem": "REGISTRO_OCORRENCIA",
        "origem": NOME_ENTIDADE,
        "conteudo": evento,
        "id_incidente": id_incidente,
        "lamport": lamport_clock,
        "vetor": list(vector_clock)
    }

    enviar_seguro(cliente, msg_registro)
    print(f"[SC] Registrando ocorrência | Incidente={id_incidente} | Evento={evento}")


def liberar_secao_critica(cliente, id_incidente):
    incrementar_relogio_local()

    msg_release = {
        "tipo_mensagem": "RELEASE_SC",
        "origem": NOME_ENTIDADE,
        "id_incidente": id_incidente,
        "lamport": lamport_clock,
        "vetor": list(vector_clock)
    }

    enviar_seguro(cliente, msg_release)
    print(f"[SC] RELEASE_SC enviado | Incidente={id_incidente} | Lamport={lamport_clock}")


#EVENTOS OPERACIONAIS
def enviar_evento_operacional(cliente, conn_broker, evento, id_incidente):
    incrementar_relogio_local()

    msg_evento = {
        "tipo_mensagem": "EVENTO",
        "origem": NOME_ENTIDADE,
        "conteudo": evento,
        "id_incidente": id_incidente,
        "timestamp": time.ctime(),
        "lamport": lamport_clock,
        "vetor": list(vector_clock)
    }

    enviar_seguro(cliente, msg_evento)
    publicar_status_broker(conn_broker, evento, id_incidente)

    print(f"[APOIO] EVENTO enviado | Incidente={id_incidente} | {evento} | Lamport={lamport_clock}")


def executar_atendimento(cliente, conn_broker, chamado):
    global aguardando_grant

    id_incidente = chamado.get("id_incidente", "DESCONHECIDO")
    tipo_incidente = chamado.get("tipo_incidente", "DESCONHECIDO")
    detalhes = chamado.get("detalhes_incidente", {})
    coordenadas = detalhes.get("coordenadas", {"x": "?", "y": "?"})

    print(
        f"[APOIO] Novo chamado recebido -> "
        f"Incidente={id_incidente} | Tipo={tipo_incidente} | "
        f"Local=({coordenadas.get('x')},{coordenadas.get('y')})"
    )

    eventos = [
        f"Deslocamento logístico iniciado para o incidente {id_incidente}.",
        f"Suporte técnico em andamento no incidente {id_incidente}.",
        f"Área organizada e isolada no incidente {id_incidente}.",
        f"Suporte operacional finalizado no incidente {id_incidente}."
    ]

    for evento in eventos:
        enviar_evento_operacional(cliente, conn_broker, evento, id_incidente)

        solicitar_secao_critica(cliente, id_incidente)

        while aguardando_grant:
            time.sleep(0.3)

        registrar_no_banco(cliente, evento, id_incidente)
        liberar_secao_critica(cliente, id_incidente)

        time.sleep(2)

    incrementar_relogio_local()
    msg_status = {
        "tipo_mensagem": "EVENTO",
        "origem": NOME_ENTIDADE,
        "conteudo": f"Unidade de apoio disponível novamente após atendimento do incidente {id_incidente}.",
        "id_incidente": id_incidente,
        "timestamp": time.ctime(),
        "lamport": lamport_clock,
        "vetor": list(vector_clock)
    }

    enviar_seguro(cliente, msg_status)
    publicar_status_broker(
        conn_broker,
        f"Unidade de apoio disponível novamente após atendimento do incidente {id_incidente}.",
        id_incidente
    )

    print(f"[APOIO] Incidente {id_incidente} concluído. Unidade voltou a aguardar chamados.")


def processar_chamados(cliente, conn_broker):
    while True:
        chamado = None

        with lock_fila:
            if fila_chamados:
                chamado = fila_chamados.pop(0)

        if chamado:
            executar_atendimento(cliente, conn_broker, chamado)

        time.sleep(0.5)


#INICIALIZAÇÃO
def iniciar_apoio():
    global lider_atual, aguardando_grant

    print("[APOIO] Localizando Central de Coordenação...")

    info_central = consultar_servidor_nomes(NOME_CENTRAL)

    if not info_central or "erro" in info_central:
        print("[ERRO] Central não encontrada!")
        return

    ip_central = info_central["ip"]
    porta_central = info_central["porta"]

    cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    conn_broker = conectar_broker()
    if conn_broker is None:
        print("[ERRO] Unidade de Apoio não conseguiu iniciar sem ActiveMQ.")
        return

    try:
        cliente.connect((ip_central, porta_central))
        print(f"[APOIO] Conectado à Central em {ip_central}:{porta_central}")
        print("[APOIO] Status: unidade logística ativa e aguardando chamados.")

        incrementar_relogio_local()
        msg_online = {
            "tipo_mensagem": "EVENTO",
            "origem": NOME_ENTIDADE,
            "conteudo": "Suporte logístico pronto, aguardando acionamento.",
            "timestamp": time.ctime(),
            "lamport": lamport_clock,
            "vetor": list(vector_clock)
        }
        enviar_seguro(cliente, msg_online)
        publicar_status_broker(conn_broker, "Suporte logístico pronto, aguardando acionamento.", None)

        listener = ListenerApoio()
        conn_broker.set_listener("", listener)
        conn_broker.subscribe(
            destination=FILA_DESPACHO,
            id=1,
            ack="auto"
        )

        print(f"[APOIO] Aguardando comandos do broker em {FILA_DESPACHO}...")

        thread_chamados = threading.Thread(
            target=processar_chamados,
            args=(cliente, conn_broker),
            daemon=True
        )
        thread_chamados.start()

        buffer = ""

        while True:
            data = cliente.recv(4096)

            if not data:
                print("[APOIO] Conexão encerrada pela Central.")
                break

            buffer += data.decode("utf-8")

            while "\n" in buffer:
                linha, buffer = buffer.split("\n", 1)
                linha = linha.strip()

                if not linha:
                    continue

                try:
                    msg = json.loads(linha)
                except json.JSONDecodeError:
                    print("[APOIO] Mensagem inválida recebida, ignorando...")
                    continue

                atualizar_relogio_ao_receber(
                    msg.get("lamport", 0),
                    msg.get("vetor", [0] * NUM_PROCESSOS)
                )

                tipo = msg.get("tipo_mensagem")
                lider_atual = msg.get("lider_atual", lider_atual)

                if tipo == "GRANT_SC":
                    aguardando_grant = False
                    print(f"[SC] GRANT_SC recebido! Acesso liberado para incidente {id_incidente_em_registro}.")

                elif tipo == "LIDER_ELEITO":
                    print(f"[APOIO] Novo líder reconhecido: {lider_atual}")

                else:
                    print(f"[APOIO] Msg de {msg.get('origem', 'DESCONHECIDO')}: {msg.get('conteudo', '')}")

                print(f"[RELÓGIOS] Lamport={lamport_clock} | Vetor={vector_clock} | Líder={lider_atual}")

            time.sleep(0.2)

    except Exception as e:
        print(f"[ERRO] Falha na comunicação da Unidade de Apoio: {e}")

    finally:
        try:
            cliente.close()
        except Exception:
            pass

        try:
            conn_broker.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    iniciar_apoio()