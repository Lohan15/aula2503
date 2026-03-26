import os
import time
import multiprocessing

# ===============================
# Consolidação dos resultados (MANTIDA ORIGINAL)
# ===============================
def consolidar_resultados(resultados):
    total_linhas = 0
    total_palavras = 0
    total_caracteres = 0

    contagem_global = {
        "erro": 0,
        "warning": 0,
        "info": 0
    }

    for r in resultados:
        total_linhas += r["linhas"]
        total_palavras += r["palavras"]
        total_caracteres += r["caracteres"]

        for chave in contagem_global:
            contagem_global[chave] += r["contagem"][chave]

    return {
        "linhas": total_linhas,
        "palavras": total_palavras,
        "caracteres": total_caracteres,
        "contagem": contagem_global
    }

# ===============================
# Processamento de arquivo (MANTIDA ORIGINAL)
# ===============================
def processar_arquivo(caminho):
    with open(caminho, "r", encoding="utf-8") as f:
        conteudo = f.readlines()

    total_linhas = len(conteudo)
    total_palavras = 0
    total_caracteres = 0

    contagem = {
        "erro": 0,
        "warning": 0,
        "info": 0
    }

    for linha in conteudo:
        palavras = linha.split()
        total_palavras += len(palavras)
        total_caracteres += len(linha)

        for p in palavras:
            if p in contagem:
                contagem[p] += 1

        # Simulação de processamento pesado (MANTIDA)
        for _ in range(1000):
            pass

    return {
        "linhas": total_linhas,
        "palavras": total_palavras,
        "caracteres": total_caracteres,
        "contagem": contagem
    }

# ===============================
# Worker (Consumidor para Paralelismo)
# ===============================
def worker(input_queue, output_queue):
    while True:
        caminho = input_queue.get()
        if caminho is None:  # Sinal de parada
            input_queue.task_done()
            break
        resultado = processar_arquivo(caminho)
        output_queue.put(resultado)
        input_queue.task_done()


def executar_paralelo(pasta, num_processos):
    arquivos = [os.path.join(pasta, f) for f in os.listdir(pasta)]
    
    input_queue = multiprocessing.JoinableQueue()
    output_queue = multiprocessing.Queue()

    inicio = time.time()

    # Criar processos consumidores
    processos = []
    for _ in range(num_processos):
        p = multiprocessing.Process(target=worker, args=(input_queue, output_queue))
        p.daemon = True
        p.start()
        processos.append(p)

    # Produtor: Alimenta a fila com os arquivos
    for arq in arquivos:
        input_queue.put(arq)

    # Adiciona "veneno" (None) para encerrar cada worker
    for _ in range(num_processos):
        input_queue.put(None)

    # Aguarda a conclusão de todos os arquivos na fila
    input_queue.join()
    fim = time.time()

    # Coleta resultados da fila para consolidar
    resultados = []
    while not output_queue.empty():
        resultados.append(output_queue.get())

    resumo = consolidar_resultados(resultados)

    # Saída formatada igual ao seu print original
    print(f"\n=== EXECUÇÃO PARALELA ({num_processos} processos) ===")
    print(f"Arquivos processados: {len(resultados)}")
    print(f"Tempo total: {fim - inicio:.4f} segundos")

    print("\n=== RESULTADO CONSOLIDADO ===")
    print(f"Total de linhas: {resumo['linhas']}")
    print(f"Total de palavras: {resumo['palavras']}")
    print(f"Total de caracteres: {resumo['caracteres']}")

    print("\nContagem de palavras-chave:")
    for k, v in resumo["contagem"].items():
        print(f"  {k}: {v}")

    return resumo

# ===============================
# Main
# ===============================
if __name__ == "__main__":
    pasta = "log2"

    if os.path.exists(pasta):
        # O experimento pede para testar com 2, 4, 8 e 12 processos
        for n in [2, 4, 8, 12]:
            executar_paralelo(pasta, n)
    else:
        print(f"A pasta '{pasta}' não foi encontrada. Rode o gerador primeiro.")
