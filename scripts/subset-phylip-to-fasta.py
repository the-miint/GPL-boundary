#!/usr/bin/env python3
"""Read interleaved PHYLIP from stdin, write the first N sequences as FASTA to stdout.

Used by the FastTree parity-test regen workflow (see GUIDANCE_PARITY_TESTS.md)
to produce fixed-size FASTA inputs that both native FastTree and the
gpl-boundary FastTree adapter can be run against for byte-equal comparison.

Usage: subset-phylip-to-fasta.py <n_seqs> < input.p > output.fasta
"""
import sys


def parse_interleaved_phylip(text):
    lines = text.splitlines()
    header = lines[0].split()
    n_seqs = int(header[0])
    n_pos = int(header[1])
    names = []
    seqs = [[] for _ in range(n_seqs)]
    block_idx = 0
    row_in_block = 0
    for line in lines[1:]:
        if not line.strip():
            if row_in_block != 0:
                assert row_in_block == n_seqs, "incomplete block"
                block_idx += 1
                row_in_block = 0
            continue
        if block_idx == 0:
            assert len(line) > 10, "first-block line too short"
            names.append(line[:10].strip())
            chunk = line[10:]
        else:
            chunk = line
        seqs[row_in_block].append("".join(c for c in chunk if not c.isspace()).upper())
        row_in_block += 1
    return [(names[i], "".join(seqs[i])) for i in range(n_seqs)], n_pos


def main():
    if len(sys.argv) != 2:
        sys.exit(f"usage: {sys.argv[0]} <n_seqs> < input.p > output.fasta")
    n = int(sys.argv[1])
    alignment, n_pos = parse_interleaved_phylip(sys.stdin.read())
    for name, seq in alignment[:n]:
        assert len(seq) == n_pos, f"{name}: {len(seq)} != {n_pos}"
        sys.stdout.write(f">{name}\n{seq}\n")


if __name__ == "__main__":
    main()
