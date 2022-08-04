#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from destination_databend_py import DestinationDatabendPy

if __name__ == "__main__":
    DestinationDatabendPy().run(sys.argv[1:])
