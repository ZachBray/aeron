package io.aeron.archive.client;

public class AeronArchiveVersion
 implements io.aeron.version.Version
{
    public static final String VERSION = "1.45.0-SNAPSHOT";
    public static final int MAJOR_VERSION = 1;
    public static final int MINOR_VERSION = 45;
    public static final int PATCH_VERSION = 0;
    public static final String GIT_SHA = "3f2a93c330";

    @Override
    public String toString()
    {
        return VERSION;
    }

    public int majorVersion()
    {
        return MAJOR_VERSION;
    }

    public int minorVersion()
    {
        return MINOR_VERSION;
    }

    public int patchVersion()
    {
        return PATCH_VERSION;
    }

    public String gitSha()
    {
        return GIT_SHA;
    }
}
