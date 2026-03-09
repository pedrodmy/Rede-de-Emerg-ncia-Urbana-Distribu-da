import time
import json
import random
import stomp

# CONFIGURAÇÕES DO PROCESSO
PROCESS_ID = 3
NUM_PROCESSOS = 8

lamport_clock = 0
vector_clock = [0] * NUM_PROCESSOS

NOME_ENTIDADE = "SENSOR_INCIDENTE"

#  CONFIGURAÇÕES DO MIDDLEWARE
BROKER_IP = "localhost"
BROKER_PORT = 61613
FILA_INCIDENTES = "/queue/incidentes.detectados"

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


#CONEXÃO COM O ACTIVEMQ
def conectar_broker():
    try:
        conn = stomp.Connection([(BROKER_IP, BROKER_PORT)])
        conn.connect(wait=True)
        print(f"[{NOME_ENTIDADE}] Conectado ao ActiveMQ em {BROKER_IP}:{BROKER_PORT}")
        return conn
    except Exception as e:
        print(f"[ERRO] Falha ao conectar ao ActiveMQ: {e}")
        return None


#FUNÇÕES DE APOIO 
def gerar_coordenadas():
    incrementar_relogio_local()
    return {
        "x": random.randint(0, 100),
        "y": random.randint(0, 100)
    }


def menu_incidentes():
    print("\n========== MENU DE INCIDENTES ==========")
    print("1 - Incêndio")
    print("2 - Acidente de veículo")
    print("3 - Assalto")
    print("4 - Desabamento")
    print("5 - Emergência médica")
    print("0 - Encerrar sensor")
    print("=======================================\n")


def mapear_opcao_para_incidente(opcao):
    incidentes = {
        "1": "INCENDIO",
        "2": "ACIDENTE_VEICULO",
        "3": "ASSALTO",
        "4": "DESABAMENTO",
        "5": "EMERGENCIA_MEDICA"
    }
    return incidentes.get(opcao)


def montar_mensagem_incidente(tipo_incidente, numero_incidente):
    incrementar_relogio_local()
    coordenadas = gerar_coordenadas()

    incrementar_relogio_local()
    msg_incidente = {
        "tipo_mensagem": "INCIDENTE",
        "origem": NOME_ENTIDADE,
        "conteudo": {
            "id_incidente": f"INC-{numero_incidente}",
            "tipo_incidente": tipo_incidente,
            "descricao": f"Incidente do tipo {tipo_incidente} detectado pelo sensor.",
            "coordenadas": coordenadas,
            "gravidade": "ALTA" if tipo_incidente in ["INCENDIO", "DESABAMENTO"] else "MEDIA",
            "timestamp_fisico": time.ctime()
        },
        "lamport": lamport_clock,
        "vetor": list(vector_clock)
    }

    return msg_incidente


#SENSOR COM ACTIVEMQ
def iniciar_sensor():
    print(f"[{NOME_ENTIDADE}] Iniciando sensor com middleware ActiveMQ...")
    print(f"[{NOME_ENTIDADE}] Fila de publicação: {FILA_INCIDENTES}")

    conn = conectar_broker()

    if conn is None:
        print("[ERRO] Não foi possível iniciar o sensor sem conexão com o ActiveMQ.")
        return

    print(f"[{NOME_ENTIDADE}] Sensor ativo e pronto para registrar incidentes.\n")

    contador_incidentes = 1

    try:
        while True:
            menu_incidentes()
            opcao = input("Selecione o incidente desejado: ").strip()

            if opcao == "0":
                print(f"\n[{NOME_ENTIDADE}] Encerrando sensor de incidentes...")
                break

            tipo_incidente = mapear_opcao_para_incidente(opcao)

            if not tipo_incidente:
                print("[AVISO] Opção inválida. Tente novamente.")
                continue

            msg_incidente = montar_mensagem_incidente(tipo_incidente, contador_incidentes)

            incrementar_relogio_local()
            msg_incidente["lamport"] = lamport_clock
            msg_incidente["vetor"] = list(vector_clock)

            try:
                conn.send(
                    destination=FILA_INCIDENTES,
                    body=json.dumps(msg_incidente)
                )

                conteudo = msg_incidente["conteudo"]
                print(
                    f"[{NOME_ENTIDADE}] Incidente publicado com sucesso -> "
                    f"ID={conteudo['id_incidente']} | "
                    f"TIPO={conteudo['tipo_incidente']} | "
                    f"LOCAL=({conteudo['coordenadas']['x']},{conteudo['coordenadas']['y']}) | "
                    f"Lamport={lamport_clock}"
                )

                contador_incidentes += 1

            except Exception as e:
                print(f"[ERRO] Falha ao publicar incidente no ActiveMQ: {e}")

            print()

    except KeyboardInterrupt:
        print(f"\n[{NOME_ENTIDADE}] Encerrado manualmente.")

    finally:
        try:
            conn.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    iniciar_sensor()