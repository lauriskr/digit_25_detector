package ee.digit25.detector.domain.device;

import ee.digit25.detector.domain.device.external.DeviceRequester;
import ee.digit25.detector.domain.device.external.api.Device;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class DeviceValidator {

    private final DeviceRequester requester;

    public boolean isValid(String macAddress) {
        log.info("Validating device {}", macAddress);
        
        Device device = requester.getDevice(macAddress);
        if (device == null) {
            log.error("Failed to validate device: {}", macAddress);
            return false;
        }
        
        return !device.getIsBlacklisted();
    }

    public Map<String, Boolean> areValid(List<String> macAddresses) {
        log.info("Validating {} devices", macAddresses.size());

        Map<String, Device> devices = requester.getDevices(macAddresses);
        return macAddresses.stream()
            .collect(java.util.stream.Collectors.toMap(
                mac -> mac,
                mac -> {
                    Device device = devices.get(mac);
                    if (device == null) {
                        log.error("Failed to validate device: {}", mac);
                        return false;
                    }
                    return !device.getIsBlacklisted();
                }
            ));
    }

    public CompletableFuture<Map<String, Boolean>> areValidAsync(List<String> macAddresses) {
        return requester.getDevicesAsync(macAddresses)
            .thenApply(devices -> macAddresses.stream()
                .collect(java.util.stream.Collectors.toMap(
                    mac -> mac,
                    mac -> {
                        Device device = devices.get(mac);
                        if (device == null) {
                            log.error("Failed to validate device: {}", mac);
                            return false;
                        }
                        return !device.getIsBlacklisted();
                    }
                )))
            .orTimeout(5000, TimeUnit.MILLISECONDS)
            .exceptionally(ex -> {
                log.error("Error in async device validation", ex);
                return macAddresses.stream()
                    .collect(java.util.stream.Collectors.toMap(
                        mac -> mac,
                        mac -> false
                    ));
            });
    }
}
