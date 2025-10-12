#!/usr/bin/env python3
# diff_noplus.py
# Uso:
#   python diff_noplus.py ruta/ANTES.py ruta/DESPUES.py
# Opcional:
#   python diff_noplus.py ruta/ANTES.py ruta/DESPUES.py --context 3 --ignore-space

import argparse
import sys
from difflib import SequenceMatcher

ANSI = {
    "reset": "\033[0m",
    "add": "\033[32m",     # verde
    "del": "\033[31m",     # rojo
    "mod": "\033[36m",     # cian
    "hdr": "\033[35m",     # magenta
    "dim": "\033[90m",
}

def colorize(s, code, use_color):
    return f"{ANSI[code]}{s}{ANSI['reset']}" if use_color else s

def read_file(path, ignore_space=False):
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        lines = f.read().splitlines()
    if ignore_space:
        # Normaliza espacios para comparación, pero conserva original para mostrar
        norm = [" ".join(l.strip().split()) for l in lines]
        return lines, norm
    return lines, lines[:]

def print_block_header(a_path, b_path, use_color):
    print(colorize(f"=== DIFF (sin '+') ===", "hdr", use_color))
    print(colorize(f"A: {a_path}", "dim", use_color))
    print(colorize(f"B: {b_path}", "dim", use_color))
    print()

def show_context(a_lines, b_lines, a_lo, a_hi, b_lo, b_hi, ctx, use_color):
    # imprime una cabecera de bloque con líneas de contexto si se quiere
    if ctx <= 0:
        return
    a_start = max(a_lo - ctx, 0)
    b_start = max(b_lo - ctx, 0)
    a_ctx = a_lines[a_start:a_lo]
    b_ctx = b_lines[b_start:b_lo]
    # usa las de B como contexto (equivalente), pero no marcas cambios
    for i, line in enumerate(b_ctx):
        ln = b_start + i + 1
        print(colorize(f"    ... {ln:>5}: {line}", "dim", use_color))

def main():
    p = argparse.ArgumentParser(description="Diff sin '+' para imprimir en consola.")
    p.add_argument("before")
    p.add_argument("after")
    p.add_argument("--context", type=int, default=0, help="Líneas de contexto (default 0)")
    p.add_argument("--no-color", action="store_true", help="Desactiva colores ANSI")
    p.add_argument("--ignore-space", action="store_true", help="Ignora diferencias de espacios para comparar")
    args = p.parse_args()

    use_color = sys.stdout.isatty() and not args.no_color

    a_lines, a_cmp = read_file(args.before, ignore_space=args.ignore_space)
    b_lines, b_cmp = read_file(args.after, ignore_space=args.ignore_space)

    print_block_header(args.before, args.after, use_color)

    sm = SequenceMatcher(None, a_cmp, b_cmp, autojunk=False)
    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag == "equal":
            continue

        # Contexto antes del bloque
        if args.context > 0:
            show_context(a_lines, b_lines, i1, i2, j1, j2, args.context, use_color)

        if tag == "insert":
            for off, line in enumerate(b_lines[j1:j2]):
                ln = j1 + off + 1
                print(colorize(f"ADD {ln:>6}: ", "add", use_color) + f"{line}")
        elif tag == "delete":
            for off, line in enumerate(a_lines[i1:i2]):
                ln = i1 + off + 1
                print(colorize(f"DEL {ln:>6}: ", "del", use_color) + f"{line}")
        elif tag == "replace":
            a_chunk = a_lines[i1:i2]
            b_chunk = b_lines[j1:j2]
            # Si tamaños coinciden, muestra línea a línea
            if len(a_chunk) == len(b_chunk):
                for k in range(len(a_chunk)):
                    a_ln = i1 + k + 1
                    b_ln = j1 + k + 1
                    print(colorize(f"MOD {a_ln:>6} → {b_ln:<6}", "mod", use_color))
                    print("   OLD   : " + a_chunk[k])
                    print("   NEW   : " + b_chunk[k])
            else:
                # Tamaños distintos: muestra bloque completo
                print(colorize(f"MOD A[{i1+1}:{i2}] → B[{j1+1}:{j2}]", "mod", use_color))
                if a_chunk:
                    print("   OLD ▼")
                    for idx, line in enumerate(a_chunk, start=i1+1):
                        print(f"   {idx:>6}: {line}")
                if b_chunk:
                    print("   NEW ▼")
                    for idx, line in enumerate(b_chunk, start=j1+1):
                        print(f"   {idx:>6}: {line}")

        # Separador entre bloques
        print(colorize("-" * 80, "dim", use_color))

if __name__ == "__main__":
    main()
