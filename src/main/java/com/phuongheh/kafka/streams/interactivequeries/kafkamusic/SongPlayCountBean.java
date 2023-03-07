package com.phuongheh.kafka.streams.interactivequeries.kafkamusic;

import java.util.Objects;

public class SongPlayCountBean {
    private String artist;
    private String album;
    private String name;
    private Long plays;

    public SongPlayCountBean() {
    }

    public SongPlayCountBean(String artist, String album, String name, Long player) {
        this.artist = artist;
        this.album = album;
        this.name = name;
        this.plays = player;
    }

    public String getArtist() {
        return artist;
    }

    public void setArtist(String artist) {
        this.artist = artist;
    }

    public String getAlbum() {
        return album;
    }

    public void setAlbum(String album) {
        this.album = album;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getPlays() {
        return plays;
    }

    public void setPlays(Long plays) {
        this.plays = plays;
    }

    @Override
    public String toString() {
        return "SongPlayCountBean{" +
                "artist='" + artist + '\'' +
                ", album='" + album + '\'' +
                ", name='" + name + '\'' +
                ", player=" + plays +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SongPlayCountBean)) return false;

        SongPlayCountBean that = (SongPlayCountBean) o;

        if (getArtist() != null ? !getArtist().equals(that.getArtist()) : that.getArtist() != null) return false;
        if (getAlbum() != null ? !getAlbum().equals(that.getAlbum()) : that.getAlbum() != null) return false;
        if (getName() != null ? !getName().equals(that.getName()) : that.getName() != null) return false;
        return getPlays() != null ? getPlays().equals(that.getPlays()) : that.getPlays() == null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(artist, album, name, plays);
    }
}
