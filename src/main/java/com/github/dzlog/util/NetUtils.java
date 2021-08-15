package com.github.dzlog.util;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.net.*;
import java.util.Collections;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * NetUtils
 */
public class NetUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(NetUtils.class);

    private static volatile Optional<String> optHostname = Optional.absent();

    @Nonnull
    public static String determineHostName() {
        try {
            if (!optHostname.isPresent()) {
                final String hostName = InetAddress.getLocalHost().getHostName();
                if (Strings.isNullOrEmpty(hostName)) {
                    throw new UnknownHostException("Unable to lookup localhost, got back empty hostname");
                }
                if (Strings.nullToEmpty(hostName).equals("0.0.0.0")) {
                    throw new UnknownHostException("Unable to resolve correct hostname saw bad host");
                }
                optHostname = Optional.of(hostName);
                return optHostname.get();
            } else {
                return optHostname.get();
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to find hostname " + e.getMessage(), e);
        }
    }

    @Nonnull
    public static String determineHostName(@Nonnull final String defaultValue) {
        checkNotNull(defaultValue, "Unable to use default value of null for hostname");
        if (!optHostname.isPresent()) {
            return determineHostName(); // this will get and save it.
        }
        return optHostname.or(defaultValue);
    }

    @Nullable
    public static String determineIpAddress() {
        try {
            return Collections.list(NetworkInterface.getNetworkInterfaces()).stream()
                    .filter(NetUtils::isValidInterface)
                    .flatMap(i -> Collections.list(i.getInetAddresses()).stream())
                    .filter(ip -> ip instanceof Inet4Address && ip.isSiteLocalAddress())
                    .findFirst().orElse(InetAddress.getLocalHost())
                    .getHostAddress();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private static boolean isValidInterface(NetworkInterface net) {
        try {
            return net.isUp() && !net.isVirtual() && !net.isLoopback()
                    && !StringUtils.startsWith(net.getName(), "flannel")
                    && !StringUtils.startsWith(net.getName(), "utun");
        } catch (SocketException e) {
            return false;
        }
    }

    public static boolean isYarnUrlConnectable(String yarnUrl) {
        String yarnRestUrl = yarnUrl + "/ws/v1/cluster/metrics";
        LOGGER.info("校验yarnUrl地址是否可用：" + yarnRestUrl);
        try {
            URL url = new URL(yarnRestUrl);
            HttpURLConnection huc = (HttpURLConnection) url.openConnection();
            int responseCode = huc.getResponseCode();
            if (HttpURLConnection.HTTP_OK == responseCode) {
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            LOGGER.info("校验yarnUrl地址不可用：" + yarnRestUrl);
            LOGGER.error(yarnUrl + e.getMessage());
            return false;
        }
    }

    public static boolean isRunning(String address) {
        String[] arr = StringUtils.split(address, ":");
        if (arr.length == 2 && StringUtils.isNumeric(arr[1])) {
            return isRunning(arr[0], Integer.valueOf(arr[1]));
        } else {
            throw new IllegalArgumentException("address: " + address + ", format ip:port");
        }
    }

    public static boolean isRunning(String host, int port) {
        Socket server = null;
        try {
            server = new Socket(host, port);
            return true;
        }
        catch (Exception ex) {
            return false;
        }
        finally {
            if (server != null) {
                try {
                    server.close();
                }
                catch (IOException ex) {
                }
            }
        }
    }

    public static String getIpAddr(HttpServletRequest request) {
        String ip = request.getHeader("x-real-ip");
        if (StringUtils.isNotEmpty(ip)){
            return ip;
        }
        ip = request.getHeader("x-forwarded-for");
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("Proxy-Client-IP");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("WL-Proxy-Client-IP");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getRemoteAddr();
        }
        return ip;
    }
}
