✅ Что делает этот BOM:
Управляет версиями зависимостей через <dependencyManagement>.
Позволяет импортировать его в другие проекты, не требуя наследования.
Обеспечивает согласованность версий библиотек в разных модулях.

📌 Как использовать BOM в другом проекте:

````
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>ru.borisov.group</groupId>
            <artifactId>borisov-starter-parent</artifactId>
            <version>1.0.0</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
````

После этого вы можете подключать зависимости без указания версии:
````
<dependencies>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-web</artifactId>
    </dependency>
    <dependency>
        <groupId>org.projectlombok</groupId>
        <artifactId>lombok</artifactId>
    </dependency>
</dependencies>
````

ST-003:

✅ Что теперь включает BOM:
Spring Boot, Cloud, Authorization Server — с управлением версий.
Lombok, MapStruct, Jackson — с управлением версий.
PostgreSQL, MongoDB, Redis — драйверы.
JUnit, Mockito, WireMock — для тестирования.
Все зависимости можно теперь использовать без указания версии, если BOM импортирован.