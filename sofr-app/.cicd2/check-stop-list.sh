#!/usr/bin/env bash
# Цвета
TXT_RED="\e[31m"
TXT_YEL="\e[1;33m"
TXT_CLEAR="\e[0m"
# Получаем id автора
authorId=$(curl --fail --silent --show-error --header "PRIVATE-TOKEN:$SOFR_APP_READ_API_PERSONAL_TOKEN" "$CI_API_V4_URL/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID" | jq --exit-status '.author.id')
# Формируем стоп-лист
# Евгений Судьяров (7061 1816)
stoppableIds=('7061' '1816')
# Проверяем наличие id автора в стоп-листе
if [[ "${array[@]}" =~ "$authorId" ]];
    then
        echo -e "${TXT_YEL}Для автослияния требуется одобрение куратора${TXT_CLEAR}"
        #TODO Код ниже по бо́льшей части дубликат из approver-check.sh
        #TODO Нужно унифицировать проверку в отдельный файл. Пусть пишет результат в файл, а что делать с результатом решает клиентский код
        # Проверяем наличие валидного апрувера
        approvers=$(curl --fail --silent --show-error --header "PRIVATE-TOKEN:$SOFR_APP_READ_API_PERSONAL_TOKEN" "$CI_API_V4_URL/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/approvals" | jq --exit-status '.approved_by')
        if [[ $approvers == '[]' ]];
          then 
              echo -e "${TXT_RED}Нет одобрений${TXT_CLEAR}"
              exit 1
          else # Уполномоченные сотрудники: Жилин, Суриков, Бутузов, Минаева, Шентеряков, Козырев
            approver_ids=$(echo $approvers | jq --exit-status '.[].user.id')
            k=0
            for id in $approver_ids; 
              do
                if [[ $id -eq 3742 || $id -eq 955 || $id -eq 3247 || $id -eq 9965 || $id -eq 1965 || $id -eq 10669 ]];
                  then
                  approver_name=$(echo $approvers | jq --exit-status ".[$k].user.username")
                  echo -e "${TXT_YEL}MR одобрен пользователем $approver_name${TXT_CLEAR}"
                  exit 0
                fi
                ((k++))
              done
        fi
    else
      echo -e "${TXT_YEL}Одобрение не требуется${TXT_CLEAR}"
      exit 0
fi