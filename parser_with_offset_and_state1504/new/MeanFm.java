package ru.x5.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class MeanFm {
    private String matnr;
    private String meinh;
    private String lfnum;
    private String ean11;
}
