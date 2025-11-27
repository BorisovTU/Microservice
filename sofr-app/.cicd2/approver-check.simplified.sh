TXT_RED="\e[31m"
TXT_YEL="\e[1;33m"
TXT_CLEAR="\e[0m"
# Экспромт-решение от ИИ. Рабочее, если апрувер один
# Доработать на работу с несколькими апруверами (и выводить имя апрувера, а не только айди)
# Либо вынести проверку апрувера в отдельную джобу в начало конвейера
json=$(curl --fail --silent --show-error --header "PRIVATE-TOKEN:$SOFR_APP_READ_API_PERSONAL_TOKEN" "$CI_API_V4_URL/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/approvals")
id=$(echo "$json" | grep -oP '"id":\K\d+' | head -1)
if [[ $id -eq 13799 || $id -eq 12420 || $id -eq 955 || $id -eq 13797 || $id -eq 13383 || $id -eq 9965 || $id -eq 1965 ]];
  then
  echo -e "${TXT_YEL}MR одобрен пользователем c id: $id${TXT_CLEAR}"
  echo -e "${TXT_YEL}Джоб пропущен${TXT_CLEAR}"
  echo 'yes' > $CI_PROJECT_DIR/is_approved.txt
  exit 0
fi
echo 'no' > $CI_PROJECT_DIR/is_approved.txt