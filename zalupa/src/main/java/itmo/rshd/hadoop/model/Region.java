package itmo.rshd.hadoop.model;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.geo.GeoJsonPolygon;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.ArrayList;
import java.util.List;

@Data
@Document(collection = "regions")
public class Region {
    @Id
    private String id;
    private String name;
    private RegionType type;
    private String parentRegionId; // For hierarchical structure (district -> city -> country)
    private GeoJsonPolygon boundaries; // Changed to GeoJsonPolygon
    private double averageSocialRating; // Calculated field
    private int populationCount;
    private int importantPersonsCount; // Count of people with IMPORTANT or VIP status
    private boolean underThreat; // Flag for regions that may be targeted
    private List<User> users = new ArrayList<>(); // Added field to store users within the region
    
    public enum RegionType {
        DISTRICT,
        CITY,
        REGION,
        COUNTRY
    }
}
