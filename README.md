# MagmaDB

**MagmaDB** é um banco de dados NoSQL in-memory escrito em Python puro (biblioteca padrão), inspirado no Redis. Oferece alta performance, persistência via WAL + snapshots e replicação líder-seguidor.

> ⚡ Zero dependências externas — apenas Python 3.10+

## Funcionalidades

- **Engine LRU O(1)** — get/set com evicção LRU usando lista duplamente encadeada + hash map
- **Protocolo RESP** — parser textual compatível com o protocolo do Redis
- **Persistência WAL** — Write-Ahead Log com `fsync` por comando; recuperação linear no boot
- **BGSAVE** — snapshot assíncrono do dataset via `ThreadPoolExecutor` + `pickle`
- **Replicação Líder-Seguidor** — propagação de escritas em tempo real com `full-sync` inicial
- **Cliente assíncrono** — servidor `asyncio` na porta 6379, conexões concorrentes

## Comandos

| Comando | Descrição | Suportado em slave |
|---|---|---|
| `PING` | Testa conexão | ✅ |
| `SET key value` | Define chave-valor | ❌ (retorna `-READONLY`) |
| `GET key` | Obtém valor | ✅ |
| `DELETE key` | Remove chave | ❌ (retorna `-READONLY`) |
| `EXISTS key` | Verifica existência | ✅ |
| `DBSIZE` | Número de chaves | ✅ |
| `FLUSHALL` | Limpa tudo | ❌ (retorna `-READONLY`) |
| `BGSAVE` | Snapshot manual | ❌ (retorna `-READONLY`) |
| `INFO` | Informações do servidor | ✅ |

## Início rápido

```bash
# Master (porta 6379)
python -m magmadb.server --port 6379 --data-dir ./data

# Slave (porta 6380)
python -m magmadb.server --port 6380 --slaveof 127.0.0.1:6379 --data-dir ./data_slave
```

### Testar com socket

```bash
python -c "
import socket
s = socket.socket(); s.connect(('127.0.0.1',6379))
s.sendall(b'*3\r\n\$3\r\nSET\r\n\$4\r\nnome\r\n\$5\r\nmagmadb\r\n')
print('SET:', s.recv(4096))
s.sendall(b'*2\r\n\$3\r\nGET\r\n\$4\r\nnome\r\n')
print('GET:', s.recv(4096))
"
```

**Saída esperada:**
```
SET: b'+OK\r\n'
GET: b'$5\r\nmagmadb\r\n'
```

## Parâmetros

| Argumento | Padrão | Descrição |
|---|---|---|
| `--port` | `6379` | Porta TCP |
| `--max-keys` | `10000` | Máx. de chaves antes de LRU |
| `--data-dir` | `data` | Diretório p/ WAL e snapshots |
| `--slaveof` | — | Modo slave: `HOST:PORT` do master |
| `--bgsave-interval` | `300` | Segundos entre BGSAVEs (0 = desliga) |
| `--verbose` | `false` | Log detalhado |

## Arquitetura

```
src/
├── magmadb/
│   ├── __init__.py       # Export público
│   ├── engine.py         # VoltEngine — LRU O(1) thread-safe
│   ├── protocol.py       # RESP — parser/encoder do protocolo
│   ├── storage.py        # Wal + Snapshotter — persistência
│   ├── replication.py    # ReplicaManager + ReplicaClient
│   └── server.py         # Servidor asyncio + CLI
├── test_quick.py         # Testes unitários + integração
└── requirements.txt      # (vazio — sem dependências externas)
```

### Engine

`VoltEngine` implementa LRU com **lista duplamente encadeada** + **dict** para O(1) em get/set. Toda operação é protegida por `threading.Lock`.

### Persistência

1. **WAL (`wal.log`):** cada `SET`/`DELETE` é registrado em disco com `fsync` imediato antes de alterar a memória. No boot, o arquivo é lido linearmente para reconstruir o estado.
2. **BGSAVE (`dump.volt`):** dispara um snapshot assíncrono via `run_in_executor`, serializando o dataset com `pickle` em um arquivo temporário e renomeando atomicamente.

### Replicação

- **Master:** mantém uma lista de conexões de slaves. Após cada escrita no WAL, propaga o comando para todos os slaves via `asyncio`.
- **Slave:** no boot, conecta ao master, envia `REPLICATE`, recebe um `full-sync` (todos os dados) e entra em modo streaming. Comandos de escrita de clientes comuns são rejeitados com `-ERR READONLY`.

## Testes

```bash
python test_quick.py
```

Os testes cobrem: engine LRU, WAL recovery, snapshot/restore e integração do servidor com comandos via socket.

## Persistência (exemplo)

```bash
# Terminal 1 — servidor
python -m magmadb.server --port 6379 --data-dir ./data

# Terminal 2 — escreve dados e derruba o servidor
python -c "
import socket, time
s = socket.socket(); s.connect(('127.0.0.1',6379))
s.sendall(b'*3\r\n\$3\r\nSET\r\n\$1\r\nx\r\n\$5\r\nhello\r\n'); s.recv(4096)
s.sendall(b'*3\r\n\$3\r\nSET\r\n\$1\r\ny\r\n\$5\r\nworld\r\n'); s.recv(4096)
"

# Deruba o servidor (Ctrl+C) e reinicia
python -m magmadb.server --port 6379 --data-dir ./data

# Os dados ainda estão lá
python -c "
import socket
s = socket.socket(); s.connect(('127.0.0.1',6379))
s.sendall(b'*2\r\n\$3\r\nGET\r\n\$1\r\nx\r\n'); print(s.recv(4096))
s.sendall(b'*2\r\n\$3\r\nGET\r\n\$1\r\ny\r\n'); print(s.recv(4096))
"
```

## Licença

MIT
