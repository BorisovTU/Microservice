package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.message;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.annotations.UpdateTimestamp;
import org.hibernate.type.SqlTypes;

import java.time.ZonedDateTime;
import java.util.UUID;

@Entity
@Table(name = "message")
@Getter
@Setter
@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
@DiscriminatorColumn(name = "message_type", discriminatorType = DiscriminatorType.STRING)
public abstract class BaseMessage {
    @Id
    @GeneratedValue
    @Column(name = "id")
    private UUID id;

    @Column(name = "caid")
    private Long caid;

    @Column(name = "json", columnDefinition = "json", nullable = false)
    @JdbcTypeCode(SqlTypes.JSON)
    private String json;

    @Column(name = "status", nullable = false)
    private Integer status = 0;

    @CreationTimestamp
    @Column(name = "create_date_time", nullable = false, updatable = false)
    private ZonedDateTime createDateTime;

    @UpdateTimestamp
    @Column(name = "update_date_time", nullable = false)
    private ZonedDateTime updateDateTime;

    public abstract String getMessageType();

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public Long getCaid() {
        return caid;
    }

    public void setCaid(Long caid) {
        this.caid = caid;
    }

    public String getJson() {
        return json;
    }

    public void setJson(String json) {
        this.json = json;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public ZonedDateTime getCreateDateTime() {
        return createDateTime;
    }

    public void setCreateDateTime(ZonedDateTime createDateTime) {
        this.createDateTime = createDateTime;
    }

    public ZonedDateTime getUpdateDateTime() {
        return updateDateTime;
    }

    public void setUpdateDateTime(ZonedDateTime updateDateTime) {
        this.updateDateTime = updateDateTime;
    }
}
