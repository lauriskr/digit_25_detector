package ee.digit25.detector.domain.device.external;

import ee.digit25.detector.domain.device.external.api.Device;
import ee.digit25.detector.domain.device.external.api.DeviceApi;
import ee.digit25.detector.domain.device.external.api.DeviceApiProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import retrofit2.Response;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class DeviceRequester {

    private final DeviceApi api;
    private final DeviceApiProperties properties;
    private static final int BATCH_SIZE = 25;

    public Device getDevice(String macAddress) {
        try {
            log.info("Requesting device: {}", macAddress);
            Response<Device> response = api.get(properties.getToken(), macAddress).execute();
            
            if (!response.isSuccessful()) {
                log.error("Failed to get device: {} - {}", macAddress, response.code());
                return null;
            }
            
            return response.body();
        } catch (IOException e) {
            log.error("Error getting device: {}", macAddress, e);
            return null;
        }
    }

    public Map<String, Device> getDevices(List<String> macAddresses) {
        if (macAddresses == null || macAddresses.isEmpty()) {
            return Collections.emptyMap();
        }

        // Remove duplicates
        List<String> uniqueMacs = new ArrayList<>(new HashSet<>(macAddresses));
        
        // Split into batches
        List<List<String>> batches = new ArrayList<>();
        for (int i = 0; i < uniqueMacs.size(); i += BATCH_SIZE) {
            batches.add(uniqueMacs.subList(i, Math.min(i + BATCH_SIZE, uniqueMacs.size())));
        }

        Map<String, Device> result = new HashMap<>();
        
        for (List<String> batch : batches) {
            try {
                log.info("Requesting batch of {} devices", batch.size());
                Response<List<Device>> response = api.get(properties.getToken(), batch).execute();
                
                if (!response.isSuccessful()) {
                    log.error("Failed to get devices batch - {}", response.code());
                    continue;
                }
                
                List<Device> devices = response.body();
                if (devices != null) {
                    devices.forEach(device -> result.put(device.getMac(), device));
                }
            } catch (IOException e) {
                log.error("Error getting devices batch", e);
            }
        }
        
        return result;
    }

    public CompletableFuture<Map<String, Device>> getDevicesAsync(List<String> macAddresses) {
        return CompletableFuture.supplyAsync(() -> getDevices(macAddresses))
            .orTimeout(5000, TimeUnit.MILLISECONDS)
            .exceptionally(ex -> {
                log.error("Error in async device batch request", ex);
                return Collections.emptyMap();
            });
    }
}
