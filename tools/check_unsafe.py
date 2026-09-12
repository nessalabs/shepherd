"""Check the documented Rust unsafe-site inventory; not a soundness proof."""
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
CHAR = re.compile(r"(?:b)?'(?:\\(?:x[0-9a-fA-F]{2}|u\{[0-9a-fA-F_]+\}|.)|[^'\\\n])'")
RAW = re.compile(r'(?:br|r)(#*)"')


def unsafe_count(source):
    # Mask comments (including nested block comments) and normal/raw strings.
    # Char literals cannot contain the whole unsafe keyword; lifetimes stay intact.
    out = []
    i = 0
    while i < len(source):
        if source.startswith('//', i):
            end = source.find('\n', i)
            i = len(source) if end < 0 else end
        elif source.startswith('/*', i):
            depth = 1
            i += 2
            while i < len(source) and depth:
                if source.startswith('/*', i):
                    depth += 1
                    i += 2
                elif source.startswith('*/', i):
                    depth -= 1
                    i += 2
                else:
                    i += 1
        elif match := CHAR.match(source, i):
            i += len(match[0])
        elif match := RAW.match(source, i):
            ending = '"' + match[1]
            end = source.find(ending, i + len(match[0]))
            i = len(source) if end < 0 else end + len(ending)
        elif source[i] == '"':
            i += 1
            while i < len(source):
                if source[i] == '\\':
                    i += 2
                elif source[i] == '"':
                    i += 1
                    break
                else:
                    i += 1
        else:
            out.append(source[i])
            i += 1
            continue
        out.append(' ')
    return len(re.findall(r'\bunsafe\b', ''.join(out)))


def inventory():
    result = {}
    for base in ('crates', 'fixtures'):
        for path in (ROOT / base).rglob('*.rs'):
            count = unsafe_count(path.read_text())
            if count:
                result[path.relative_to(ROOT).as_posix()] = count
    return result


def main():
    actual = inventory()
    doc = (ROOT / 'docs/UNSAFE_CODE.md').read_text()
    expected = {path: int(count) for path, count in
                re.findall(r'^\| `([^`]+\.rs)` \| (\d+) \|', doc, re.M)}
    if actual != expected:
        print('Unsafe inventory differs from docs/UNSAFE_CODE.md. Review and justify the change.')
        for path in sorted(actual.keys() | expected.keys()):
            if actual.get(path) != expected.get(path):
                print(f'{path}: documented={expected.get(path, 0)} actual={actual.get(path, 0)}')
        return 1
    print(f'Unsafe inventory matches: {sum(actual.values())} sites in {len(actual)} files.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
