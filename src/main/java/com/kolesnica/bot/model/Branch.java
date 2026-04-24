package com.kolesnica.bot.model;

public record Branch(
        long id,
        String city,
        String district,
        String address,
        String phone,
        String schedule,
        boolean is24x7,
        Double latitude,
        Double longitude
) {
    public String shortLabel() {
        return "📍 " + district + ": " + address;
    }

    public String cardText() {
        return "📍 " + city + ", " + district + "\n"
                + "🏢 " + address + "\n"
                + "🕒 " + schedule + "\n"
                + "📞 " + phone;
    }
}
