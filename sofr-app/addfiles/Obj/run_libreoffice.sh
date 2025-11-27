#!/bin/sh
echo "Run libreoffice converter"

if [ "$#" -ne 1 ]
then
   echo "Incorrect number of arguments"
   exit 1
fi

export LANG='ru_RU.UTF-8'

unix_params_file=`realpath $1`
readarray -t ARGS <$unix_params_file

unix_out_dir=`realpath ${ARGS[0]} | tr -d '\n\r'`
unix_in_file=`realpath ${ARGS[1]} | tr -d '\n\r'`
out_file_name=`echo ${ARGS[2]} | tr -d '\n\r'`
format=`echo ${ARGS[3]} | tr -d '\n\r'`

file_name="${unix_in_file##*/}"
file_only_name="${file_name%.*}"

# https://superuser.com/questions/647464/how-to-get-the-display-number-i-was-assigned-by-x
export DISPLAY=`ps -u $(id -u) -o pid= | xargs -I PID -r cat /proc/PID/environ 2> /dev/null | tr '\0' '\n' | grep -oP "^DISPLAY=\K(.+)" | sort -u | head -n 1`
export XAUTHORITY=`ps -u $(id -u) -o pid= | xargs -I PID cat /proc/PID/environ 2>/dev/null | tr '\0' '\n' | grep -oP '^XAUTHORITY=\K(.+)' | sort -u | head -n 1`

libreoffice --headless --invisible --convert-to $format --outdir $unix_out_dir $unix_in_file  

mv $unix_out_dir/$file_only_name.${out_file_name##*.} $unix_out_dir/$out_file_name


