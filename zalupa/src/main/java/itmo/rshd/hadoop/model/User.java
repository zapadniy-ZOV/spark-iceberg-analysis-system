package itmo.rshd.hadoop.model;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Document(collection = "users")
public class User {
    @Id
    private String id;
    private String username;
    private String password;
    private String fullName;
    private double socialRating;
    private SocialStatus status;
    private GeoLocation currentLocation;
    private String regionId;
    private String districtId;
    private String countryId;
    private boolean active;
    private long lastLocationUpdateTimestamp;

    public enum SocialStatus {
        LOW, // Low social status
        REGULAR, // Regular citizen
        IMPORTANT, // Important person
        VIP // Very important person with high privileges
    }
}