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
}