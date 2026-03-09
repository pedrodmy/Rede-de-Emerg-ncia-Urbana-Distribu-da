import socket
import json
import time
import threading
import stomp
from config_rede import IP_SERVIDOR_NOMES, PORTA_SERVIDOR_NOMES, PORTA_MONITORAMENTO

#CONFIGURAÇÕES DO PROCESSO
PROCESS_ID = 2
NUM_PROCESSOS = 8

lamport_clock = 0
vector_clock = [0] * NUM_PROCESSOS

NOME_ENTIDADE = "CENTRO_MONITORAMENTO"
NOME_CENTRAL = "CENTRAL_COORDENACAO"

lider_atual = "DESCONHECIDO"
central_ativa = False

#CONFIGURAÇÕES DO ACTIVEMQ
BROKER_IP = "localhost"
BROKER_PORT = 61613

FILA_INCIDENTES_DETECTADOS = "/queue/incidentes.detectados"
FILA_INCIDENTES_VALIDADOS = "/queue/incidentes.validados"

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


#SERVIDOR DE NOMES
def registrar_no_servidor_nomes(porta):
    IP_NS, PORTA_NS = IP_SERVIDOR_NOMES, PORTA_SERVIDOR_NOMES
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    incrementar_relogio_local()
    msg_registro = {
        "tipo_requisicao": "REGISTRAR",
        "nome": NOME_ENTIDADE,
        "porta": porta,
        "servico": "Monitoramento de Incidentes",
        "lamport": lamport_clock,
        "vetor": list(vector_clock)
    }

    try:
        sock.sendto(json.dumps(msg_registro).encode("utf-8"), (IP_NS, PORTA_NS))
        data, _ = sock.recvfrom(2048)
        resposta = json.loads(data.decode("utf-8"))

        atualizar_relogio_ao_receber(
            resposta.get("lamport", 0),
            resposta.get("vetor", [0] * NUM_PROCESSOS)
        )

        print(f"[MONITORAMENTO] Registrado no Servidor de Nomes com confirmação. | Lamport={lamport_clock}")

    except Exception as e:
        print(f"[ERRO] Falha ao registrar no Servidor de Nomes: {e}")

    finally:
        sock.close()


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
        print(f"[ERRO] Falha ao consultar o Servidor de Nomes: {e}")
        return None

    finally:
        sock.close()


#FUNÇÕES DE APOIO
def enviar_tcp_seguro(sock_tcp, mensagem):
    try:
        sock_tcp.send((json.dumps(mensagem) + "\n").encode("utf-8"))
        return True
    except Exception as e:
        print(f"[ERRO] Falha ao enviar mensagem TCP: {e}")
        return False


def validar_incidente(msg_sensor):
    if msg_sensor.get("tipo_mensagem") != "INCIDENTE":
        return False

    conteudo = msg_sensor.get("conteudo", {})

    campos_obrigatorios = ["id_incidente", "tipo_incidente", "descricao", "coordenadas", "gravidade", "timestamp_fisico"]
    for campo in campos_obrigatorios:
        if campo not in conteudo:
            return False

    coordenadas = conteudo.get("coordenadas", {})
    if "x" not in coordenadas or "y" not in coordenadas:
        return False

    return True


def montar_incidente_monitorado(msg_sensor):
    incrementar_relogio_local()

    return {
        "tipo_mensagem": "INCIDENTE_MONITORADO",
        "origem": NOME_ENTIDADE,
        "sensor_origem": msg_sensor.get("origem", "DESCONHECIDO"),
        "conteudo": msg_sensor["conteudo"],
        "status_monitoramento": "VALIDADO",
        "timestamp_monitoramento": time.ctime(),
        "lamport": lamport_clock,
        "vetor": list(vector_clock)
    }


def anunciar_status_inicial(sock_tcp):
    incrementar_relogio_local()

    msg_status = {
        "tipo_mensagem": "EVENTO",
        "origem": NOME_ENTIDADE,
        "conteudo": "Centro de monitoramento ativo e acompanhando incidentes urbanos.",
        "timestamp": time.ctime(),
        "lamport": lamport_clock,
        "vetor": list(vector_clock)
    }

    enviar_tcp_seguro(sock_tcp, msg_status)


#ELEIÇÃO DE LÍDER
def iniciar_eleicao(sock_tcp):
    global lider_atual

    incrementar_relogio_local()

    msg_eleicao = {
        "tipo_mensagem": "ELEICAO",
        "origem": NOME_ENTIDADE,
        "process_id": PROCESS_ID,
        "conteudo": "Solicitação de eleição de líder.",
        "timestamp": time.ctime(),
        "lamport": lamport_clock,
        "vetor": list(vector_clock)
    }

    if enviar_tcp_seguro(sock_tcp, msg_eleicao):
        print(f"[ELEIÇÃO] Solicitação de eleição enviada à Central. | Lamport={lamport_clock}")

    lider_atual = NOME_CENTRAL


#ACTIVEMQ
def conectar_broker():
    try:
        conn = stomp.Connection([(BROKER_IP, BROKER_PORT)])
        conn.connect(wait=True)
        print(f"[MONITORAMENTO] Conectado ao ActiveMQ em {BROKER_IP}:{BROKER_PORT}")
        return conn
    except Exception as e:
        print(f"[ERRO] Falha ao conectar ao ActiveMQ: {e}")
        return None


class ListenerIncidentes(stomp.ConnectionListener):
    def __init__(self, conn_broker, tcp_central):
        self.conn_broker = conn_broker
        self.tcp_central = tcp_central

    def on_error(self, frame):
        print(f"[MONITORAMENTO][BROKER][ERRO] {frame.body}")

    def on_message(self, frame):
        global lamport_clock, vector_clock

        try:
            msg_sensor = json.loads(frame.body)
        except json.JSONDecodeError:
            print("[MONITORAMENTO] Mensagem inválida recebida do ActiveMQ. Ignorando...")
            return

        atualizar_relogio_ao_receber(
            msg_sensor.get("lamport", 0),
            msg_sensor.get("vetor", [0] * NUM_PROCESSOS)
        )

        print(f"[MONITORAMENTO] Incidente recebido do broker | Lamport={lamport_clock}")

        incrementar_relogio_local()

        if not validar_incidente(msg_sensor):
            print("[MONITORAMENTO] Incidente inválido descartado.")
            return

        conteudo = msg_sensor["conteudo"]
        print(
            f"[MONITORAMENTO] Incidente validado -> "
            f"ID={conteudo['id_incidente']} | "
            f"TIPO={conteudo['tipo_incidente']} | "
            f"GRAVIDADE={conteudo['gravidade']} | "
            f"LOCAL=({conteudo['coordenadas']['x']},{conteudo['coordenadas']['y']})"
        )

        incidente_monitorado = montar_incidente_monitorado(msg_sensor)

        incrementar_relogio_local()
        incidente_monitorado["lamport"] = lamport_clock
        incidente_monitorado["vetor"] = list(vector_clock)

        # Publica no middleware para a Central consumir
        try:
            self.conn_broker.send(
                destination=FILA_INCIDENTES_VALIDADOS,
                body=json.dumps(incidente_monitorado)
            )
            print(f"[ENVIO] Incidente monitorado publicado em {FILA_INCIDENTES_VALIDADOS} | Lamport={lamport_clock}")
        except Exception as e:
            print(f"[ERRO] Falha ao publicar incidente validado no ActiveMQ: {e}")



# MONITORAMENTO
def iniciar_monitoramento():
    global lider_atual, central_ativa

    PORTA_LOCAL = PORTA_MONITORAMENTO

    # Registrar no Servidor de Nomes
    registrar_no_servidor_nomes(PORTA_LOCAL)

    # Descobrir a Central
    print("[MONITORAMENTO] Localizando Central de Coordenação...")
    info_central = consultar_servidor_nomes(NOME_CENTRAL)

    if not info_central or "erro" in info_central:
        print("[ERRO] Central não encontrada!")
        return

    # Conectar à Central via TCP
    tcp_central = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        tcp_central.connect((info_central["ip"], info_central["porta"]))
        central_ativa = True

        print(f"[MONITORAMENTO] Conectado à Central em {info_central['ip']}:{info_central['porta']}")
        print("[MONITORAMENTO] Status: monitoramento ativo, aguardando incidentes do ActiveMQ.")

        # Anunciar status inicial
        anunciar_status_inicial(tcp_central)

        # Iniciar eleição simples
        iniciar_eleicao(tcp_central)

        # Conectar ao broker e consumir incidentes detectados
        conn_broker = conectar_broker()
        if conn_broker is None:
            print("[ERRO] Não foi possível iniciar o monitoramento sem conexão com o ActiveMQ.")
            return

        listener = ListenerIncidentes(conn_broker, tcp_central)
        conn_broker.set_listener("", listener)

        conn_broker.subscribe(
            destination=FILA_INCIDENTES_DETECTADOS,
            id=1,
            ack="auto"
        )

        print(f"[MONITORAMENTO] Aguardando incidentes do broker em {FILA_INCIDENTES_DETECTADOS}...")

        while True:
            time.sleep(1)

    except Exception as e:
        print(f"[ERRO] Falha no Centro de Monitoramento: {e}")
        central_ativa = False

    finally:
        try:
            tcp_central.close()
        except Exception:
            pass

        try:
            conn_broker.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    iniciar_monitoramento()