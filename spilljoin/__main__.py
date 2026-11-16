import argparse

from spilljoin import __version__


def main():
    parser = argparse.ArgumentParser(prog="spilljoin")
    parser.add_argument("--version", action="version", version=__version__)
    parser.parse_args()


if __name__ == "__main__":
    main()
