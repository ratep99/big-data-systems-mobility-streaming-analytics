package models;
import lombok.*;

import java.text.DecimalFormat;

@Builder
@Getter
@Setter
@ToString
@EqualsAndHashCode
@AllArgsConstructor
@NoArgsConstructor
public class Vehicle {
    private String timestamp;
    private String id;
    private String type;
    private double latitude;
    private double longitude;
    private double speed_kmh;
    private double acceleration;
    private double distance;
    private double odometer;
    private double pos;
    public Double getLatitudeRounded(int brojDecimala){
        String formatString = "#.";
        for(int i=0; i<brojDecimala; i++)
        {
            formatString+="#";
        }

        DecimalFormat df = new DecimalFormat(formatString);
        return Double.parseDouble(df.format(latitude));
    }
    public Double getLongitudeRounded(int brojDecimala) {
        String formatString = "#.";
        for (int i = 0; i < brojDecimala; i++) {
            formatString += "#";
        }

        DecimalFormat df = new DecimalFormat(formatString);
        return Double.parseDouble(df.format(longitude));
    }
}