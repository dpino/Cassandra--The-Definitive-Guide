package com.cassandraguide.clients.old;

import java.math.BigInteger;
import java.security.MessageDigest;

/**
 * OUTPUT:
 *
 * PasswordIN= havebadpass :: Stored Password=e1a31eee2136eb73e8e47f9e9d13ab0d
 * Password STORED Bytes= [B@1d9f953d :: Password IN Bytes=[B@1034bb5
 * equal? true
 */

/**
 *
 */
public class TestPasswordMD5 {

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {

        test();
        // doIt();

    }

    public static void doIt() throws Exception {

        String password = "havebadpass";
        MessageDigest m = MessageDigest.getInstance("MD5");
        m.reset();
        m.update(password.getBytes());
        byte[] digest = m.digest();
        BigInteger bigInt = new BigInteger(1, digest);
        String hashtext = bigInt.toString(16);
        while (hashtext.length() < 32) {
            hashtext = "0" + hashtext;
        }
        System.out.println("S: " + hashtext);

    }

    public static void test() throws Exception {

        String hash = "e1a31eee2136eb73e8e47f9e9d13ab0d";

        System.setProperty("jsmith", hash);

        String password = "havebadpass";
        String passwordStored = System.getProperty("jsmith");

        System.out.println("PasswordIN= " + password + " :: Stored Password="
                + passwordStored);

        System.out.println("Password STORED Bytes= "
                + passwordStored.getBytes() + " :: Password IN Bytes="
                + MessageDigest.getInstance("MD5").digest(password.getBytes()));

        MessageDigest m = MessageDigest.getInstance("MD5");
        m.reset();
        m.update(password.getBytes());
        byte[] digest = m.digest();
        BigInteger bigInt = new BigInteger(1, digest);
        String hashtext = bigInt.toString(16);
        while (hashtext.length() < 32) {
            hashtext = "0" + hashtext;
        }

        boolean authenticated = passwordStored.equals(hashtext);

        System.out.println("equal? " + authenticated);
    }
}
