#!/bin/bash
echo "Enter domain username (ex. d.s.pavlov):"
read NAME
echo "Enter domain password:"
read -s PASSWORD
export http_proxy="http://tcsbank\\$NAME:$PASSWORD@mwg.gtm.tcsbank.ru:8080"
export https_proxy="http://tcsbank\\$NAME:$PASSWORD@mwg.gtm.tcsbank.ru:8080"
echo "Proxy set successfully."
exec $SHELL –i

