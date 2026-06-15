# Multiplicação de Matrizes com MPI, Servidor e Filas Persistentes

Este projeto implementa um trabalho acadêmico em Python usando `mpi4py`, seguindo a ideia do exemplo em C/MPI fornecido pelo professor: dividir a multiplicação de matrizes entre processos diferentes.

A diferença principal é que, além do processo mestre e dos trabalhadores, existe um processo servidor intermediário. Esse servidor recebe as tarefas, registra as mensagens em uma fila persistente local e encaminha os dados para os trabalhadores. Depois, ele também recebe os resultados dos trabalhadores e encaminha de volta para a origem.

## Objetivo

Multiplicar duas matrizes quadradas `A` e `B`, ambas de dimensão `n x n`, usando MPI.

A multiplicação é dividida por linhas:

```text
C = A * B

Cada tarefa enviada ao trabalhador contém linhas de A
e a matriz B completa.
```

Cada trabalhador calcula apenas sua parte da matriz `C`. No fim, a origem junta todas as partes e valida o resultado comparando com uma multiplicação sequencial feita em Python.

## Dependências

Ambiente usado e testado neste projeto:

- Python `3.13.5`
- Microsoft MPI `10.1.12498.52`
- `mpi4py` `4.1.2`
- Windows Package Manager `winget`

Instale as dependências no Windows com estes comandos:

```powershell
winget install --id Python.Python.3.13 --version 3.13.5 --exact --accept-source-agreements --accept-package-agreements
winget install --id Microsoft.msmpi --version 10.1.12498.52 --exact --accept-source-agreements --accept-package-agreements --silent
python -m pip install mpi4py==4.1.2
```

Depois da instalação, feche o terminal atual e abra um terminal novo para carregar o `mpiexec` no `PATH`.

Verifique a instalação com estes comandos:

```powershell
python --version
python -m pip show mpi4py
mpiexec -help
python -c "from mpi4py import MPI; print(MPI.Get_library_version())"
```

Resultados esperados:

```text
Python 3.13.5
Name: mpi4py
Version: 4.1.2
Microsoft MPI Startup Program [Version 10.1.12498.52]
Microsoft MPI 10.1.12498.52
```

## Como executar

Execute com `mpiexec`.

Exemplo mínimo com 3 processos:

```bash
mpiexec -n 3 python main.py --n 4
```

Exemplo com semente para resultado reprodutível:

```bash
mpiexec -n 4 python main.py --n 5 --seed 42
```

Exemplo com matriz maior:

```bash
mpiexec -n 5 python main.py --n 8 --seed 123
```

Também é possível controlar os sleeps aleatórios:

```bash
mpiexec -n 4 python main.py --n 5 --seed 42 --sleep-min 0.1 --sleep-max 0.5
```

## Papéis dos processos

O programa usa pelo menos três tipos de processo:

- `rank 0`: origem/mestre.
- `rank 1`: servidor intermediário.
- `rank 2` em diante: trabalhadores/destinos.

Com 3 processos:

```text
rank 0 = origem/mestre
rank 1 = servidor
rank 2 = trabalhador
```

Com mais de 3 processos:

```text
rank 0 = origem/mestre
rank 1 = servidor
ranks 2, 3, 4, ... = trabalhadores
```

## Fluxo de mensagens

O fluxo principal é:

1. A origem cria as matrizes `A` e `B`.
2. A origem divide as linhas de `A` entre os trabalhadores.
3. A origem envia mensagens do tipo `TASK` para o servidor.
4. O servidor salva a mensagem em sua fila e encaminha para o trabalhador correto.
5. O trabalhador calcula suas linhas da matriz `C`.
6. O trabalhador envia uma mensagem `RESULT` para o servidor.
7. O servidor salva o resultado e encaminha para a origem.
8. A origem monta a matriz final `C`.
9. A origem envia `STOP` ao servidor.
10. O servidor encaminha `STOP` aos trabalhadores.

A comunicação entre processos é feita com MPI, usando `comm.send` e `comm.recv` da biblioteca `mpi4py`.

## Filas persistentes

Cada processo possui uma fila local em arquivo JSONL dentro da pasta `queues/`.

Exemplos:

```text
queues/process_0.jsonl
queues/process_1.jsonl
queues/process_2.jsonl
```

Cada mensagem registrada possui campos como:

```json
{
  "id": "TASK-0-2-ab12cd34ef56",
  "origem": 0,
  "destino": 2,
  "tipo": "TASK",
  "payload": {},
  "status": "pendente"
}
```

Os status usados são:

- `pendente`: mensagem registrada e ainda não concluída no processo atual.
- `enviada`: mensagem enviada por MPI.
- `processada`: mensagem consumida pelo processo atual.
- `entregue`: mensagem recebida no destino esperado.

Essa persistência é uma simulação acadêmica. Ela mostra que a mensagem fica registrada em arquivo enquanto o processo dorme temporariamente. Assim, o sleep atrasa o processamento, mas não apaga as mensagens.

## Sleeps aleatórios

Cada processo chama `time.sleep(...)` em momentos do fluxo de execução.

A duração do sleep é sorteada entre `--sleep-min` e `--sleep-max`. Por exemplo:

```bash
--sleep-min 0.1 --sleep-max 0.6
```

Quando um processo acorda, ele consulta sua fila local e informa se ainda existem mensagens pendentes. Isso simula a ideia de que uma mensagem persistente continua disponível mesmo quando o processo fica temporariamente indisponível.

## Divisão das linhas

As linhas da matriz `A` são distribuídas de forma equilibrada.

Exemplo: se `n = 5` e existem 2 trabalhadores:

```text
trabalhador 2 recebe linhas 0, 1, 2
trabalhador 3 recebe linhas 3, 4
```

Assim, o programa também funciona quando o número de linhas não é divisível exatamente pelo número de trabalhadores.

## Validação

Ao final, o processo de origem imprime:

- matriz `A`;
- matriz `B`;
- matriz `C` calculada em paralelo;
- matriz `C` calculada sequencialmente;
- mensagem de validação.

Quando tudo está correto, aparece:

```text
Validação: resultado paralelo confere com o sequencial.
```

## Arquivos do projeto

```text
main.py     Programa principal com MPI e mpi4py
README.md   Explicação do trabalho
queues/     Pasta criada automaticamente para as filas persistentes
```

## Observação

O programa não usa `multiprocessing`, `threading` nem NumPy. A multiplicação foi implementada com listas Python para deixar o código mais didático e próximo da explicação acadêmica.
