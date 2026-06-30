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

## FAQ para apresentação

### Este projeto tem apenas um arquivo Python. Como sabemos que são processos diferentes?

O fato de existir apenas um arquivo Python não significa que exista apenas um processo em execução. Neste trabalho, quem cria os processos não é uma função Python do tipo `multiprocessing`. Quem cria os processos é o programa externo `mpiexec`, instalado junto com o Microsoft MPI.

O comando usado tem esta forma:

```powershell
mpiexec -n N python main.py
```

Cada parte do comando tem uma função:

- `mpiexec`: é o inicializador de processos MPI. Ele pede ao sistema operacional para iniciar várias execuções do programa indicado.
- `-n N`: informa quantos processos devem ser criados. Se o comando for `mpiexec -n 4 python main.py`, serão iniciadas 4 execuções independentes de `python main.py`.
- `python`: é o interpretador Python que será aberto em cada processo.
- `main.py`: é o arquivo executado por cada interpretador Python.

Para o sistema operacional, isso aparece como vários processos `python.exe` separados. Cada um tem seu próprio PID, sua própria memória, suas próprias variáveis e seu próprio fluxo de execução. Eles executam o mesmo arquivo, mas não são a mesma execução.

O MPI diferencia esses processos usando o conceito de `rank`. Quando cada instância de `python main.py` inicia, a biblioteca `mpi4py` se conecta ao runtime nativo do Microsoft MPI. Esse runtime informa a cada processo qual é seu número dentro do grupo global `MPI.COMM_WORLD`. Assim, um processo recebe `rank 0`, outro recebe `rank 1`, outro recebe `rank 2`, e assim por diante.

É por isso que o código consegue usar um único arquivo e ainda assim ter comportamentos diferentes. Todos começam executando `main.py`, mas cada um lê seu próprio `rank`. Depois disso, o programa escolhe a função correta para aquele processo: origem/mestre, servidor intermediário e trabalhador.

Usamos uma dependência externa ao Python porque MPI não faz parte da biblioteca padrão do Python. O `mpi4py` é a ponte entre Python e a implementação MPI nativa instalada no sistema. No nosso ambiente, essa implementação nativa é o Microsoft MPI, que fornece o `mpiexec` e a biblioteca `msmpi.dll`. Em resumo: `mpiexec` cria os processos no sistema operacional, Microsoft MPI organiza a comunicação entre eles, e `mpi4py` permite chamar essa comunicação a partir do código Python.

Função/linhas: em `main`, `MPI.COMM_WORLD`, `comm.Get_rank()` e `comm.Get_size()` aparecem em `main.py:449-451`. O desvio por `rank`, que faz cada processo executar uma função diferente, aparece em `main.py:471-476`.

### Onde cada processo recebe seu papel de origem, servidor e trabalhador?

Os papéis são definidos pelo valor do `rank`. O rank `0` executa `origin_process`, o rank `1` executa `server_process`, e os ranks a partir de `2` executam `worker_process`.

Função/linhas: constantes dos papéis em `main.py:21-23`; desvio principal em `main.py:471-476`; funções dos papéis em `origin_process` (`main.py:235`), `server_process` (`main.py:325`) e `worker_process` (`main.py:393`).

### Onde o processo mestre cria as matrizes A e B?

A origem cria as matrizes usando a função `generate_matrix`. A semente configurável entra antes da geração para permitir testes repetíveis.

Função/linhas: `generate_matrix` em `main.py:72-74`; geração de `matrix_a` e `matrix_b` em `origin_process`, `main.py:243-245`.

### Onde as linhas da matriz A são divididas entre os trabalhadores?

A divisão é feita por `split_work`. Ela calcula uma quantidade base de linhas para cada trabalhador e distribui o resto entre os primeiros trabalhadores.

Função/linhas: `split_work` em `main.py:99-112`; chamada em `origin_process`, `main.py:250`; montagem do payload com linhas e índices em `main.py:258-266`.

### Onde a matriz B completa é enviada para cada trabalhador?

A matriz B completa entra no payload da mensagem de tarefa. Assim, cada trabalhador recebe suas linhas de A e a matriz B inteira.

Função/linhas: campo `"matriz_b"` colocado no payload em `main.py:264`; leitura pelo trabalhador em `main.py:418-421`.

### Onde é feita a comunicação entre os processos?

A comunicação real acontece com MPI usando `comm.send` e `comm.recv`.

Função/linhas: origem envia tarefa em `main.py:275`, recebe resultado em `main.py:286` e envia `STOP` em `main.py:311`; servidor recebe mensagens em `main.py:334`, encaminha tarefa em `main.py:359`, encaminha resultado em `main.py:369` e envia `STOP` aos trabalhadores em `main.py:379`; trabalhador recebe mensagem em `main.py:400` e envia resultado em `main.py:439`.

### Onde o servidor atua como intermediário de verdade?

O servidor recebe mensagens de qualquer processo, salva na fila local e decide o encaminhamento pelo tipo da mensagem: `TASK`, `RESULT` e `STOP`.

Função/linhas: loop do servidor em `server_process`, `main.py:332-384`; recebimento geral em `main.py:334`; gravação na fila em `main.py:339`; encaminhamento de tarefas em `main.py:348-360`; encaminhamento de resultados em `main.py:362-370`; encerramento em `main.py:372-384`.

### Onde os processos geram suas filas persistentes?

No início da execução, o rank `0` limpa filas antigas. Depois todos os processos criam seu próprio arquivo `queues/process_<rank>.jsonl`.

Função/linhas: `reset_queue_directory` em `main.py:119-124`; `initialize_queue` em `main.py:127-131`; chamada no início do `main` em `main.py:453-457`.

### Onde os processos escrevem e leem mensagens da fila?

A escrita usa `save_message_to_queue`. A leitura usa `load_all_messages` e `load_pending_messages`. A alteração de status usa `mark_message_status` e `mark_message_as_delivered`.

Função/linhas: escrita em `main.py:134-141`; leitura completa em `main.py:144-155`; leitura de pendentes em `main.py:158-160`; atualização de status em `main.py:163-182`.

### Onde cada mensagem recebe id, origem, destino, tipo, payload e status?

Toda mensagem é criada por `create_message`, que monta o dicionário padronizado exigido pelo trabalho.

Função/linhas: tipos e status em `main.py:27-34`; criação da mensagem em `main.py:185-194`.

### Quando o processo dorme e volta, como ele sabe para onde voltar?

O processo chama `time.sleep` dentro da função `maybe_sleep`. Quando o tempo acaba, o Python continua exatamente na próxima linha após `time.sleep`. Depois disso, a própria função consulta a fila local para listar mensagens pendentes. O ponto de retorno é preservado pelo fluxo normal do programa e pelo loop onde a chamada aconteceu.

Função/linhas: `maybe_sleep` em `main.py:197-225`; chamada inicial da origem em `main.py:241`; chamadas da origem durante envio e recebimento em `main.py:279` e `main.py:285`; chamadas do servidor em `main.py:330`, `main.py:346` e `main.py:382`; chamadas do trabalhador em `main.py:397`, `main.py:406`, `main.py:424` e `main.py:442`.

### Onde o trabalhador calcula sua parte da matriz C?

O trabalhador recebe as linhas de A, recebe a matriz B completa e chama `multiply_rows_by_matrix` para calcular apenas sua parte.

Função/linhas: algoritmo de multiplicação em `multiply_rows_by_matrix`, `main.py:77-91`; chamada no trabalhador em `main.py:426`; montagem da resposta em `main.py:429-435`.

### Onde a origem junta a matriz C final?

A origem recebe resultados do servidor, pega os índices das linhas calculadas e coloca cada linha na posição correta da matriz final.

Função/linhas: inicialização da matriz paralela em `main.py:281`; recebimento do resultado em `main.py:286`; preenchimento das linhas finais em `main.py:293-300`.

### Onde é feita a validação sequencial?

Após receber todos os resultados paralelos, a origem calcula `A * B` de forma sequencial e compara as duas matrizes.

Função/linhas: `sequential_multiply` em `main.py:94-96`; chamada e comparação final em `main.py:314-322`.

### Como o programa evita execução inválida com menos de 3 processos?

O `main` verifica a quantidade total de processos. Com menos de 3, o rank `0` imprime erro e a execução termina.

Função/linhas: verificação em `main.py:462-469`.

### Como o programa encerra todos os trabalhadores?

Depois de montar e validar a matriz C, a origem envia `STOP` ao servidor. O servidor repassa `STOP` para cada trabalhador. Cada trabalhador encerra ao receber essa mensagem.

Função/linhas: origem cria e envia `STOP` em `main.py:308-312`; servidor trata `STOP` em `main.py:372-384`; trabalhador finaliza ao receber `STOP` em `main.py:408-411`.

### Como a execução mostra no terminal qual processo fez cada ação?

Todas as mensagens de log passam pela função `log`, que imprime rank e papel do processo.

Função/linhas: função `log` em `main.py:37-39`; exemplos de uso na origem em `main.py:240`, no servidor em `main.py:329` e no trabalhador em `main.py:396`.

## Arquivos do projeto

```text
main.py     Programa principal com MPI e mpi4py
README.md   Explicação do trabalho
queues/     Pasta criada automaticamente para as filas persistentes
```

## Observação

O programa não usa `multiprocessing`, `threading` nem NumPy. A multiplicação foi implementada com listas Python para deixar o código mais didático e próximo da explicação acadêmica.
