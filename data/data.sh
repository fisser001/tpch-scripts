#!/bin/bash

if [ ! -d $1 ] && [ ! -d $2 ]; then
echo "-------------------------------------- CREATE PATH IF NOT EXIST ---------------------------------"
mkdir -p /data/mydata/raw/sf$1
cd /data/mydata/raw/sf$1
echo "-------------------------------------- CREATED PATH IF NOT EXIST ---------------------------------"
echo "-------------------------------------- Path: /data/mydata/raw/sf$ ---------------------------------"

echo "-------------------------------------- LOAD DATA  ----------------------------------------------"
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')
echo ""
wget --no-check-certificate -O sf$1.zip "$2"
echo "-------------------------------------- DATA HAS BEEN LOADED  ----------------------------------------------"
echo ""
echo "-------------------------------------- UNZIP DATA  ----------------------------------------------"
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')
echo ""
unzip sf$1.zip
echo ""
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')
echo "-------------------------------------- UNZIP DATA FINISHED ----------------------------------------------"
echo "-------------------------------------- DELETE DATA  ----------------------------------------------"
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')
echo ""
rm -fr sf$1.zip
echo ""
echo $(date '+%d/%m/%Y %H:%M:%S.%3N')
echo "-------------------------------------- DELETE DATA FINISHED ----------------------------------------------"

else
echo "Parameters have to be defined for this script. Paramater 1 for scale factor. Paramter 2 for download url"
echo "Example ./data.sh 1 \"https://blablabla.de:444/sgare.cgi/gdjsj\""
exit 2
fi