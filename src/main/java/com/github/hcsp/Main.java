package com.github.hcsp;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class Main {
    @SuppressFBWarnings("DMI_CONSTANT_DB_PASSWORD")
    public static void main(String[] args) {
        CrawlerDao dao = new MybatisCrawlerDao();
        for (int i = 0; i < 1; i++) {
            new Crawler(dao).start();
        }
    }
}
