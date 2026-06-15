import argparse
import copy
import json
import os
import random
import sys
import time
import uuid

try:
    from mpi4py import MPI
except ImportError:
    print(
        "Erro: a biblioteca mpi4py não está instalada.\n"
        "Instale com: pip install mpi4py\n"
        "Também é necessário ter uma implementação MPI instalada, como MS-MPI, MPICH ou Open MPI."
    )
    sys.exit(1)


ORIGIN_RANK = 0
SERVER_RANK = 1
FIRST_WORKER_RANK = 2
TAG_MESSAGE = 100
QUEUE_DIR = "queues"

TYPE_TASK = "TASK"
TYPE_RESULT = "RESULT"
TYPE_STOP = "STOP"

STATUS_PENDING = "pendente"
STATUS_SENT = "enviada"
STATUS_PROCESSED = "processada"
STATUS_DELIVERED = "entregue"


def log(rank, role, message):
    """Imprime uma mensagem padronizada, identificando rank e papel."""
    print(f"[rank {rank} | {role}] {message}", flush=True)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Multiplicação de matrizes com MPI, servidor intermediário e filas persistentes."
    )
    parser.add_argument("--n", type=int, default=4, help="Dimensão da matriz quadrada.")
    parser.add_argument("--seed", type=int, default=None, help="Semente aleatória para testes reprodutíveis.")
    parser.add_argument(
        "--sleep-min",
        type=float,
        default=0.10,
        help="Duração mínima dos sleeps aleatórios em segundos.",
    )
    parser.add_argument(
        "--sleep-max",
        type=float,
        default=0.60,
        help="Duração máxima dos sleeps aleatórios em segundos.",
    )
    args = parser.parse_args()

    if args.n <= 0:
        parser.error("--n deve ser maior que zero.")
    if args.sleep_min < 0 or args.sleep_max < 0:
        parser.error("--sleep-min e --sleep-max não podem ser negativos.")
    if args.sleep_max < args.sleep_min:
        parser.error("--sleep-max deve ser maior ou igual a --sleep-min.")

    return args


def generate_matrix(n, rng):
    """Gera uma matriz n x n com valores inteiros pequenos, de 0 a 9."""
    return [[rng.randint(0, 9) for _ in range(n)] for _ in range(n)]


def multiply_rows_by_matrix(rows, matrix_b):
    """Multiplica um conjunto de linhas da matriz A pela matriz B completa."""
    n = len(matrix_b)
    result = []

    for row in rows:
        result_row = []
        for column in range(n):
            value = 0
            for index in range(n):
                value += row[index] * matrix_b[index][column]
            result_row.append(value)
        result.append(result_row)

    return result


def sequential_multiply(matrix_a, matrix_b):
    """Calcula A * B sequencialmente para validar o resultado paralelo."""
    return multiply_rows_by_matrix(matrix_a, matrix_b)


def split_work(n, workers):
    """Divide as linhas de A entre trabalhadores de forma equilibrada."""
    base_rows = n // workers
    extra_rows = n % workers
    assignments = []
    current_row = 0

    for worker_index in range(workers):
        amount = base_rows + (1 if worker_index < extra_rows else 0)
        rows = list(range(current_row, current_row + amount))
        assignments.append(rows)
        current_row += amount

    return assignments


def queue_path(rank):
    return os.path.join(QUEUE_DIR, f"process_{rank}.jsonl")


def reset_queue_directory():
    """Remove filas antigas para que cada execucao comece com registros limpos."""
    os.makedirs(QUEUE_DIR, exist_ok=True)
    for filename in os.listdir(QUEUE_DIR):
        if filename.startswith("process_") and filename.endswith(".jsonl"):
            os.remove(os.path.join(QUEUE_DIR, filename))


def initialize_queue(rank):
    """Cria uma fila limpa para o processo atual nesta execucao."""
    os.makedirs(QUEUE_DIR, exist_ok=True)
    with open(queue_path(rank), "w", encoding="utf-8"):
        pass


def save_message_to_queue(rank, message):
    """Salva uma mensagem na fila persistente local do processo."""
    os.makedirs(QUEUE_DIR, exist_ok=True)
    stored_message = copy.deepcopy(message)
    stored_message["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")

    with open(queue_path(rank), "a", encoding="utf-8") as queue_file:
        queue_file.write(json.dumps(stored_message, ensure_ascii=False) + "\n")


def load_all_messages(rank):
    path = queue_path(rank)
    if not os.path.exists(path):
        return []

    messages = []
    with open(path, "r", encoding="utf-8") as queue_file:
        for line in queue_file:
            line = line.strip()
            if line:
                messages.append(json.loads(line))
    return messages


def load_pending_messages(rank):
    """Carrega mensagens ainda pendentes da fila local."""
    return [message for message in load_all_messages(rank) if message.get("status") == STATUS_PENDING]


def mark_message_status(rank, message_id, status):
    """Atualiza o status de uma mensagem no arquivo JSONL do processo."""
    messages = load_all_messages(rank)
    updated = False

    for message in messages:
        if message.get("id") == message_id:
            message["status"] = status
            message["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
            updated = True

    if updated:
        with open(queue_path(rank), "w", encoding="utf-8") as queue_file:
            for message in messages:
                queue_file.write(json.dumps(message, ensure_ascii=False) + "\n")


def mark_message_as_delivered(rank, message_id):
    """Atalho didatico para marcar uma mensagem como entregue."""
    mark_message_status(rank, message_id, STATUS_DELIVERED)


def create_message(origin, destination, message_type, payload):
    """Cria o dicionário padrão usado como mensagem entre processos MPI."""
    return {
        "id": f"{message_type}-{origin}-{destination}-{uuid.uuid4().hex[:12]}",
        "origem": origin,
        "destino": destination,
        "tipo": message_type,
        "payload": payload,
        "status": STATUS_PENDING,
    }


def maybe_sleep(rank, role, rng, sleep_min, sleep_max, context="", force=False):
    """
    Faz o processo dormir de forma aleatória.

    O sono simula indisponibilidade temporária. Como as mensagens ficam
    registradas em arquivo e também trafegam por MPI, o sleep apenas atrasa
    o processamento, sem apagar tarefas ou resultados.
    """
    if sleep_max == 0:
        return

    should_sleep = force or rng.random() < 0.45
    if not should_sleep:
        return

    duration = rng.uniform(sleep_min, sleep_max)
    detail = f" durante {duration:.2f}s"
    if context:
        detail += f" ({context})"

    log(rank, role, f"vai dormir{detail}.")
    time.sleep(duration)

    pending_messages = load_pending_messages(rank)
    if pending_messages:
        ids = ", ".join(message["id"] for message in pending_messages)
        log(rank, role, f"acordou e encontrou mensagens pendentes na fila: {ids}.")
    else:
        log(rank, role, "acordou sem mensagens pendentes na fila.")


def print_matrix(title, matrix):
    print(f"\n{title}")
    for row in matrix:
        print("  " + " ".join(f"{value:4}" for value in row))
    print(flush=True)


def origin_process(comm, size, args, rng):
    role = "origem/mestre"
    worker_ranks = list(range(FIRST_WORKER_RANK, size))
    worker_count = len(worker_ranks)

    log(ORIGIN_RANK, role, f"iniciado com {worker_count} trabalhador(es).")
    maybe_sleep(ORIGIN_RANK, role, rng, args.sleep_min, args.sleep_max, "antes de gerar as matrizes", True)

    matrix_rng = random.Random(args.seed)
    matrix_a = generate_matrix(args.n, matrix_rng)
    matrix_b = generate_matrix(args.n, matrix_rng)

    print_matrix("Matriz A:", matrix_a)
    print_matrix("Matriz B:", matrix_b)

    assignments = split_work(args.n, worker_count)
    active_tasks = 0

    for worker_rank, row_indexes in zip(worker_ranks, assignments):
        if not row_indexes:
            log(ORIGIN_RANK, role, f"trabalhador {worker_rank} não recebeu linhas, pois n é menor que a quantidade de trabalhadores.")
            continue

        rows = [matrix_a[row_index] for row_index in row_indexes]
        payload = {
            "worker_rank": worker_rank,
            "linha_inicial": row_indexes[0],
            "linhas_indices": row_indexes,
            "linhas_a": rows,
            "matriz_b": matrix_b,
            "n": args.n,
        }
        message = create_message(ORIGIN_RANK, worker_rank, TYPE_TASK, payload)

        save_message_to_queue(ORIGIN_RANK, message)
        log(
            ORIGIN_RANK,
            role,
            f"enviando tarefa {message['id']} ao servidor para o trabalhador {worker_rank}; linhas {row_indexes}.",
        )
        comm.send(message, dest=SERVER_RANK, tag=TAG_MESSAGE)
        mark_message_status(ORIGIN_RANK, message["id"], STATUS_SENT)
        active_tasks += 1

        maybe_sleep(ORIGIN_RANK, role, rng, args.sleep_min, args.sleep_max, "apos enviar uma tarefa")

    parallel_result = [[0 for _ in range(args.n)] for _ in range(args.n)]
    received_results = 0

    while received_results < active_tasks:
        maybe_sleep(ORIGIN_RANK, role, rng, args.sleep_min, args.sleep_max, "antes de aguardar resultado")
        message = comm.recv(source=SERVER_RANK, tag=TAG_MESSAGE)
        save_message_to_queue(ORIGIN_RANK, message)

        if message["tipo"] != TYPE_RESULT:
            log(ORIGIN_RANK, role, f"ignorou mensagem inesperada do tipo {message['tipo']}.")
            continue

        payload = message["payload"]
        row_indexes = payload["linhas_indices"]
        rows_c = payload["linhas_c"]

        for row_index, row_values in zip(row_indexes, rows_c):
            parallel_result[row_index] = row_values

        mark_message_as_delivered(ORIGIN_RANK, message["id"])
        received_results += 1
        log(
            ORIGIN_RANK,
            role,
            f"recebeu resultado {message['id']} com as linhas {row_indexes} ({received_results}/{active_tasks}).",
        )

    stop_message = create_message(ORIGIN_RANK, SERVER_RANK, TYPE_STOP, {"motivo": "multiplicacao finalizada"})
    save_message_to_queue(ORIGIN_RANK, stop_message)
    log(ORIGIN_RANK, role, f"enviando STOP {stop_message['id']} ao servidor.")
    comm.send(stop_message, dest=SERVER_RANK, tag=TAG_MESSAGE)
    mark_message_status(ORIGIN_RANK, stop_message["id"], STATUS_SENT)

    sequential_result = sequential_multiply(matrix_a, matrix_b)

    print_matrix("Matriz C final calculada em paralelo:", parallel_result)
    print_matrix("Matriz C calculada sequencialmente para validação:", sequential_result)

    if parallel_result == sequential_result:
        print("Validação: resultado paralelo confere com o sequencial.", flush=True)
    else:
        print("Validação: ERRO, resultado paralelo difere do sequencial.", flush=True)


def server_process(comm, size, args, rng):
    role = "servidor"
    worker_ranks = list(range(FIRST_WORKER_RANK, size))

    log(SERVER_RANK, role, "iniciado; vai intermediar tarefas e resultados.")
    maybe_sleep(SERVER_RANK, role, rng, args.sleep_min, args.sleep_max, "antes de receber mensagens", True)

    while True:
        status = MPI.Status()
        message = comm.recv(source=MPI.ANY_SOURCE, tag=TAG_MESSAGE, status=status)
        source_rank = status.Get_source()

        local_message = copy.deepcopy(message)
        local_message["status"] = STATUS_PENDING
        save_message_to_queue(SERVER_RANK, local_message)

        log(
            SERVER_RANK,
            role,
            f"recebeu mensagem {message['id']} do rank {source_rank}; tipo {message['tipo']}.",
        )
        maybe_sleep(SERVER_RANK, role, rng, args.sleep_min, args.sleep_max, "apos receber mensagem")

        if message["tipo"] == TYPE_TASK:
            destination = message["destino"]
            forwarded_message = copy.deepcopy(message)
            forwarded_message["origem"] = SERVER_RANK
            forwarded_message["status"] = STATUS_SENT

            log(
                SERVER_RANK,
                role,
                f"encaminhando tarefa {message['id']} ao trabalhador {destination}.",
            )
            comm.send(forwarded_message, dest=destination, tag=TAG_MESSAGE)
            mark_message_as_delivered(SERVER_RANK, message["id"])

        elif message["tipo"] == TYPE_RESULT:
            forwarded_message = copy.deepcopy(message)
            forwarded_message["origem"] = SERVER_RANK
            forwarded_message["destino"] = ORIGIN_RANK
            forwarded_message["status"] = STATUS_SENT

            log(SERVER_RANK, role, f"encaminhando resultado {message['id']} para a origem.")
            comm.send(forwarded_message, dest=ORIGIN_RANK, tag=TAG_MESSAGE)
            mark_message_as_delivered(SERVER_RANK, message["id"])

        elif message["tipo"] == TYPE_STOP:
            mark_message_status(SERVER_RANK, message["id"], STATUS_PROCESSED)
            log(SERVER_RANK, role, "recebeu STOP; avisando todos os trabalhadores.")

            for worker_rank in worker_ranks:
                stop_message = create_message(SERVER_RANK, worker_rank, TYPE_STOP, {"motivo": "encerramento"})
                save_message_to_queue(SERVER_RANK, stop_message)
                comm.send(stop_message, dest=worker_rank, tag=TAG_MESSAGE)
                mark_message_as_delivered(SERVER_RANK, stop_message["id"])
                log(SERVER_RANK, role, f"STOP enviado ao trabalhador {worker_rank}.")
                maybe_sleep(SERVER_RANK, role, rng, args.sleep_min, args.sleep_max, "entre envios de STOP")

            break

        else:
            mark_message_status(SERVER_RANK, message["id"], STATUS_PROCESSED)
            log(SERVER_RANK, role, f"tipo de mensagem desconhecido: {message['tipo']}.")

    log(SERVER_RANK, role, "finalizado.")


def worker_process(comm, rank, args, rng):
    role = "trabalhador"

    log(rank, role, "iniciado; aguardando tarefas do servidor.")
    maybe_sleep(rank, role, rng, args.sleep_min, args.sleep_max, "antes de aguardar tarefa", True)

    while True:
        message = comm.recv(source=SERVER_RANK, tag=TAG_MESSAGE)
        local_message = copy.deepcopy(message)
        local_message["status"] = STATUS_PENDING
        save_message_to_queue(rank, local_message)

        log(rank, role, f"recebeu mensagem {message['id']} do servidor; tipo {message['tipo']}.")
        maybe_sleep(rank, role, rng, args.sleep_min, args.sleep_max, "apos receber mensagem")

        if message["tipo"] == TYPE_STOP:
            mark_message_as_delivered(rank, message["id"])
            log(rank, role, "recebeu STOP e vai finalizar.")
            break

        if message["tipo"] != TYPE_TASK:
            mark_message_status(rank, message["id"], STATUS_PROCESSED)
            log(rank, role, f"ignorou mensagem inesperada do tipo {message['tipo']}.")
            continue

        payload = message["payload"]
        row_indexes = payload["linhas_indices"]
        rows_a = payload["linhas_a"]
        matrix_b = payload["matriz_b"]

        log(rank, role, f"processando linhas da matriz A: {row_indexes}.")
        maybe_sleep(rank, role, rng, args.sleep_min, args.sleep_max, "antes de multiplicar linhas")

        rows_c = multiply_rows_by_matrix(rows_a, matrix_b)
        mark_message_status(rank, message["id"], STATUS_PROCESSED)

        result_payload = {
            "worker_rank": rank,
            "linha_inicial": payload["linha_inicial"],
            "linhas_indices": row_indexes,
            "linhas_c": rows_c,
        }
        result_message = create_message(rank, ORIGIN_RANK, TYPE_RESULT, result_payload)

        save_message_to_queue(rank, result_message)
        log(rank, role, f"enviando resultado {result_message['id']} ao servidor; linhas {row_indexes}.")
        comm.send(result_message, dest=SERVER_RANK, tag=TAG_MESSAGE)
        mark_message_status(rank, result_message["id"], STATUS_SENT)

        maybe_sleep(rank, role, rng, args.sleep_min, args.sleep_max, "apos enviar resultado")

    log(rank, role, "finalizado.")


def main():
    args = parse_args()
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if rank == ORIGIN_RANK:
        reset_queue_directory()
    comm.Barrier()

    initialize_queue(rank)

    seed_base = args.seed if args.seed is not None else time.time_ns()
    rng = random.Random(seed_base + rank * 1009)

    if size < 3:
        if rank == ORIGIN_RANK:
            print(
                "Erro: este programa precisa de pelo menos 3 processos MPI.\n"
                "Exemplo: mpiexec -n 3 python main.py --n 4",
                flush=True,
            )
        return

    if rank == ORIGIN_RANK:
        origin_process(comm, size, args, rng)
    elif rank == SERVER_RANK:
        server_process(comm, size, args, rng)
    else:
        worker_process(comm, rank, args, rng)

    comm.Barrier()


if __name__ == "__main__":
    main()
