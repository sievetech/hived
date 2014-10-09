# *-* coding: utf-8 *-*
import sys
import argparse
import importlib


def parse_args():
    parser = argparse.ArgumentParser(description='Run process / cron')
    parser.add_argument('module_path',
                        action='store',
                        default=None,
                        help='module path as used on an import statement')
    parser.add_argument('the_rest',
                        action='store',
                        default=None,
                        nargs=argparse.REMAINDER,
                        help='the rest of stuff')
    return parser.parse_args()


def main():
    args = parse_args()
    sys.argv.remove(args.module_path)  # should not interfere with script's args
    module = importlib.import_module(args.module_path)
    exit_code = module.main()
    sys.exit(exit_code if exit_code else 0)


if __name__ == '__main__':
    main()

