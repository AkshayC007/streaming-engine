package com.sparkstreaming.streamingengine;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import twitter4j.GeoLocation;
import twitter4j.HashtagEntity;
import twitter4j.MediaEntity;
import twitter4j.Scopes;
import twitter4j.Status;
import twitter4j.SymbolEntity;
import twitter4j.URLEntity;
import twitter4j.User;
import twitter4j.UserMentionEntity;

import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class StatusBean implements  java.io.Serializable{
    //public static final long serialVersionUID = ;

    public Date createdAt;
    public long id;
    public String text;
    public int displayTextRangeStart = -1;
    public int displayTextRangeEnd = -1;
    public String source;
    public boolean isTruncated;
    public long inReplyToStatusId;
    public long inReplyToUserId;
    public boolean isFavorited;
    public boolean isRetweeted;
    public int favoriteCount;
    public String inReplyToScreenName;
    public GeoLocation geoLocation = null;
   // public Place place = null;
    // this field should be int in theory, but left as long for the serialized form compatibility - TFJ-790
    public long retweetCount;
    public boolean isPossiblySensitive;
    public String lang;

    public long[] contributorsIDs;

    public Status retweetedStatus;
    public UserMentionEntity[] userMentionEntities;
    public URLEntity[] urlEntities;
    public HashtagEntity[] hashtagEntities;
    public MediaEntity[] mediaEntities;
    public SymbolEntity[] symbolEntities;
    public long currentUserRetweetId = -1L;
    public Scopes scopes;
    public User user = null;
    public String[] withheldInCountries = null;
    public Status quotedStatus;
    public long quotedStatusId = -1L;

}