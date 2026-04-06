package ru.x5.model;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@XmlAccessorType(XmlAccessType.FIELD)
public class IDocData {
    @XmlElement(name = "EDI_DC40")
    private EdiDc40 ediDc40;

    @XmlElement(name = "_-POSDW_-E1POSTR_CREATEMULTIP")
    private PostrCreateMultip postrCreateMultip;
}