#!/bin/bash
# Program
#    this is a compile script, just for everyone easy to use.
# History
#    2014-09-26    liufan    first release

# set the erlang lib directory
export ERL_LIBS=deps

# use the rebar to compile the application
rebar compile
