package com.phuongheh.kafka.streams.interactivequeries.kafkamusic;

import java.util.Objects;

public class SongBean {
    private String artist;
    private String album;
    private String name;

    public SongBean(String artist, String album, String name) {
        this.artist = artist;
        this.album = album;
        this.name = name;
    }

    public SongBean() {
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

    @Override
    public String toString() {
        return "SongBean{" +
                "artist='" + artist + '\'' +
                ", album='" + album + '\'' +
                ", name='" + name + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SongBean)) return false;

        SongBean songBean = (SongBean) o;

        if (getArtist() != null ? !getArtist().equals(songBean.getArtist()) : songBean.getArtist() != null)
            return false;
        if (getAlbum() != null ? !getAlbum().equals(songBean.getAlbum()) : songBean.getAlbum() != null) return false;
        return getName() != null ? getName().equals(songBean.getName()) : songBean.getName() == null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(artist, album, name);
    }
}
