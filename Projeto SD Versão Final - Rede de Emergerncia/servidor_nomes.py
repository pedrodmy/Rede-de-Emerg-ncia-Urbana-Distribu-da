import socket
import json
import time
from config_rede import PORTA_SERVIDOR_NOMES
#CONFIGURAÇÃO DO PROCESSO
PROCESS_ID = 0
NUM_PROCESSOS = 8

lamport_clock = 0
vector_clock = [0] * NUM_PROCESSOS

NOME_ENTIDADE = "SERVIDOR_NOMES"
IP = "0.0.0.0"
PORTA = PORTA_SERVIDOR_NOMES

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
    else:
        print("[AVISO] Vetor de relógio recebido com tamanho inválido.")

    vector_clock[PROCESS_ID] += 1


#RESPOSTA PADRÃO
def montar_resposta_base():
    incrementar_relogio_local()
    return {
        "servidor": NOME_ENTIDADE,
        "timestamp_fisico": time.ctime(),
        "lamport": lamport_clock,
        "vetor": list(vector_clock)
    }


# SERVIDOR DE NOMES
def iniciar_servidor_nomes():
    registro = {}

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((IP, PORTA))

    print(f"[SERVIDOR DE NOMES] Ativo em {IP}:{PORTA}")
    print("[SERVIDOR DE NOMES] Status: pronto para registrar e localizar serviços distribuídos.")

    while True:
        try:
            data, addr = sock.recvfrom(4096)

            try:
                msg = json.loads(data.decode("utf-8"))
            except json.JSONDecodeError:
                print(f"[AVISO] Mensagem inválida recebida de {addr}.")
                continue

            lamport_msg = msg.get("lamport", 0)
            vetor_msg = msg.get("vetor", [0] * NUM_PROCESSOS)
            atualizar_relogio_ao_receber(lamport_msg, vetor_msg)

            tipo_req = msg.get("tipo_requisicao")

            # REGISTRO DE SERVIÇO
            if tipo_req == "REGISTRAR":
                nome = msg.get("nome")
                porta_servico = msg.get("porta")
                servico = msg.get("servico", "SERVICO_DESCONHECIDO")

                if not nome or not porta_servico:
                    resposta = montar_resposta_base()
                    resposta["status"] = "ERRO"
                    resposta["erro"] = "Campos obrigatórios ausentes para registro."
                    sock.sendto(json.dumps(resposta).encode("utf-8"), addr)
                    continue

                incrementar_relogio_local()

                registro[nome] = {
                    "ip": addr[0],
                    "porta": porta_servico,
                    "servico": servico,
                    "ultimo_lamport_registro": lamport_clock,
                    "timestamp_registro": time.ctime()
                }

                print(
                    f"[REGISTRO] {nome} ({servico}) registrado em "
                    f"{addr[0]}:{porta_servico} | Lamport={lamport_clock}"
                )

                resposta = montar_resposta_base()
                resposta["status"] = "OK"
                resposta["mensagem"] = f"Serviço {nome} registrado com sucesso."

                sock.sendto(json.dumps(resposta).encode("utf-8"), addr)

            #CONSULTA DE SERVIÇO
            elif tipo_req == "CONSULTAR":
                nome_busca = msg.get("nome")

                if not nome_busca:
                    resposta = montar_resposta_base()
                    resposta["status"] = "ERRO"
                    resposta["erro"] = "Nome do serviço não informado."
                    sock.sendto(json.dumps(resposta).encode("utf-8"), addr)
                    continue

                incrementar_relogio_local()
                resultado = registro.get(nome_busca)

                if resultado:
                    print(
                        f"[CONSULTA] Serviço encontrado: {nome_busca} -> "
                        f"{resultado['ip']}:{resultado['porta']} | Lamport={lamport_clock}"
                    )

                    resposta = montar_resposta_base()
                    resposta["status"] = "OK"
                    resposta["nome"] = nome_busca
                    resposta["ip"] = resultado["ip"]
                    resposta["porta"] = resultado["porta"]
                    resposta["servico"] = resultado["servico"]
                    resposta["timestamp_registro"] = resultado["timestamp_registro"]
                    resposta["ultimo_lamport_registro"] = resultado["ultimo_lamport_registro"]
                else:
                    print(f"[CONSULTA] Serviço não encontrado: {nome_busca} | Lamport={lamport_clock}")

                    resposta = montar_resposta_base()
                    resposta["status"] = "ERRO"
                    resposta["erro"] = "Não encontrado"
                    resposta["nome"] = nome_busca

                sock.sendto(json.dumps(resposta).encode("utf-8"), addr)

            #LISTAGEM OPCIONAL
            elif tipo_req == "LISTAR":
                incrementar_relogio_local()

                resposta = montar_resposta_base()
                resposta["status"] = "OK"
                resposta["servicos_registrados"] = registro

                print(f"[LISTAGEM] Relação de serviços enviada para {addr} | Lamport={lamport_clock}")
                sock.sendto(json.dumps(resposta).encode("utf-8"), addr)

            #REQUISIÇÃO DESCONHECIDA
            else:
                resposta = montar_resposta_base()
                resposta["status"] = "ERRO"
                resposta["erro"] = f"Tipo de requisição desconhecido: {tipo_req}"

                print(f"[AVISO] Requisição desconhecida recebida de {addr}: {tipo_req}")
                sock.sendto(json.dumps(resposta).encode("utf-8"), addr)

        except OSError as e:
            erro_windows = getattr(e, "winerror", None)
            if erro_windows == 10054:
                print("[AVISO] Cliente encerrou conexão antes da resposta.")
            else:
                print(f"[ERRO] Falha ao processar requisição: {e}")


# EXECUÇÃO
if __name__ == "__main__":
    iniciar_servidor_nomes()