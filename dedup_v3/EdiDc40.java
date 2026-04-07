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
public class EdiDc40 {
    @XmlElement(name = "TABNAM")
    private String tabnam;

    @XmlElement(name = "DOCNUM")
    private String docnum;

    @XmlElement(name = "DIRECT")
    private String direct;

    @XmlElement(name = "IDOCTYP")
    private String idoctyp;

    @XmlElement(name = "MESTYP")
    private String mestyp;

    @XmlElement(name = "SNDPOR")
    private String sndpor;

    @XmlElement(name = "SNDPRT")
    private String sndprt;

    @XmlElement(name = "SNDPRN")
    private String sndprn;

    @XmlElement(name = "RCVPOR")
    private String rcvpor;

    @XmlElement(name = "RCVPRT")
    private String rcvprt;

    @XmlElement(name = "RCVPRN")
    private String rcvprn;
}