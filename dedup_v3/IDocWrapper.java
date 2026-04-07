package ru.x5.model;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "_-POSDW_-POSTR_CREATEMULTIPLE02")
@XmlAccessorType(XmlAccessType.FIELD)
public class IDocWrapper {
    @XmlElement(name = "IDOC")
    private IDocData idoc;
}
