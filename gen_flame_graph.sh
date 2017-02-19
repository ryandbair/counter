#!/usr/bin/env bash

~/dev/FlameGraph/stackcollapse.pl out.stacks > out.folded
~/dev/FlameGraph/flamegraph.pl out.folded > out.svg
open out.svg