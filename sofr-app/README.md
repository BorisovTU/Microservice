# Кратко
Проект-репозиторий прикладной части исходного кода приложения СОФР.  
Обслуживается конвейерами [**sofr-app-builder**](https://gitlab.rshbdev.ru/rshbintech/it-invest/backoffice/sofr/it_dev/sofr-app-builder), [**sofr-app-automerger**](https://gitlab.rshbdev.ru/rshbintech/it-invest/backoffice/sofr/it_dev/sofr-app-automerger), [**sofr-app-checker**](https://gitlab.rshbdev.ru/rshbintech/it-invest/backoffice/sofr/it_dev/sofr-app-checker)

# Пользователям
У проекта имеется свой скрипт автоматизации для перепубликаций артефактов из **dev-nexus** в **prod-nexus**. Перепубликация возможна только для артефактов, раннее опубликованных конвейером [**sofr-app-builder**](https://gitlab.rshbdev.ru/rshbintech/it-invest/backoffice/sofr/it_dev/sofr-app-builder) в режиме **№2** (***build-and-public***). Для запуска перепубликации необходимо указать ветку релиза, артефакт которого отправляется в ПРОД, и запустить конвейер с параметрами:
- **ARTIFACT_TAG_NAME** - метка, под которой был ранее опубликован конкретный кумулятив в **dev-nexus**.
- **PROM** в значении ***true***

Результатом работы алгоритма будет публикация указанного артефакта в репозиторий **maven-external-staged** и создание тикета в [**support-board**](https://gitlab.rshbdev.ru/rshbintech/support-board/-/issues) для сотрудников *AppFarm*, т.к. само размещение таких артефактов в **prod-nexus** производится вручную. В тикете будет ссылка на мультизадачный пайплайн с различными этапами, которые должны пройти со статусом ***passed***, либо ***passed with warnings***.  
  
В проекте применяется конвейер для **merge requests**, который проверяет наличие валидных *аппруверов*.
Так как действие **'approve'** не является отдельным триггером для события **'merge_request_event'**, привязать вызов конвейера к этому действию возможности нет. Поэтому после "одобрения", необходимо будет перезапустить пайплайн заново, чтобы проверка прошла успешно.

# DevOps-инженерам
Алгоритм выгружает проект [**sofr-app-builder**](https://gitlab.rshbdev.ru/rshbintech/it-invest/backoffice/sofr/it_dev/sofr-app-builder) в отдельный подкаталог во время выполнения на раннере и использует плейбуки **extract-and-public.yml** и **rehash-and-notify.yml**. Выгрузка происходит по тегу *stable-\*.\**
