package by.webapp.kvstorage.service;

import by.webapp.kvstorage.exception.FailedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;

@Component
public class RollbackService {

    private static final Logger logger = LogManager.getLogger(RollbackService.class);
    private final DistributedService service;

    @Autowired
    public RollbackService(DistributedService service) {
        this.service = service;
    }

    public void rollback(int counter, String... parameters) {
        if (counter == 0) {
            throw new FailedException("There are not changed nodes for rollback");
        }
        if (parameters.length == 2) {
            service.sendDelete(counter, true,
                    parameters[0], parameters[1]);
        } else {
            service.distributeDeleting(counter, true, parameters[0]);
        }
        logger.info("Rollback successfully executed");
        throw new FailedException("Rollback successfully executed");
    }

    public void rollback(Object object, int counter, HttpMethod method, String... parameters) {
        if (counter == 0) {
            throw new FailedException("There are not changed nodes for rollback");
        }
        switch (method) {
            case DELETE:
                service.sendPost(object, counter, true,
                        parameters[0]);
                break;
            case PUT:
                if (parameters.length == 2) {
                    service.sendUpdate(object, counter, true,
                            parameters[0], parameters[1]);
                } else {
                    service.sendUpdate(object, counter, true,
                            parameters[0]);
                }
                break;
        }
        final String message = "Rollback for " + (method == HttpMethod.PUT ? "PUT" :
                "DELETE") + " request  successfully executed.";
        logger.info(message);
        throw new FailedException(message);
    }

}