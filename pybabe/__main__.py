
import optparse
from . import Babe
import sys

if __name__ == "__main__":
    parser = optparse.OptionParser()
    parser.add_option("--input", help="Input file")
    parser.add_option("--output", default=sys.stdout, help='Output file')
    options, remainder = parser.parse_args()
    babe = Babe()
    if not options.input:
        parser.error("--input required")
    babe.pull(options.input, 'Input').push(options.output)
