package org.example;
public class Pais {
    private String codigoPais = "---";
    private String nombrePais = "unknown";
    private String capitalPais = "unknown";
    private String region = "unknown";
    private int poblacion = 0;
    private double latitud = 0;
    private double longitud = 0;

    public String getCodigoPais() {
        return codigoPais;
    }

    public void setCodigoPais(String codigoPais) {
        this.codigoPais = codigoPais;
    }

    public String getNombrePais() {
        return nombrePais;
    }

    public void setNombrePais(String nombrePais) {
        this.nombrePais = nombrePais;
    }

    public String getCapitalPais() {
        return capitalPais;
    }

    public void setCapitalPais(String capitalPais) {
        this.capitalPais = capitalPais;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public int getPoblacion() {
        return poblacion;
    }

    public void setPoblacion(int poblacion) {
        this.poblacion = poblacion;
    }

    public double getLatitud() {
        return latitud;
    }

    public void setLatitud(double latitud) {
        this.latitud = latitud;
    }

    public double getLongitud() {
        return longitud;
    }

    public void setLongitud(double longitud) {
        this.longitud = longitud;
    }
    @Override
    public String toString() {
        return "Pais{" +
                "codigoPais='" + codigoPais + '\'' +
                ", nombrePais='" + nombrePais + '\'' +
                ", capitalPais='" + capitalPais + '\'' +
                ", region='" + region + '\'' +
                ", poblacion=" + poblacion +
                ", latitud=" + latitud +
                ", longitud=" + longitud +
                '}';
    }
}