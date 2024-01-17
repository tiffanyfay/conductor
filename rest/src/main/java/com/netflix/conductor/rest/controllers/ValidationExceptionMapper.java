/*
 * Copyright 2020 Conductor Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.rest.controllers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import com.netflix.conductor.common.validation.ErrorResponse;
import com.netflix.conductor.common.validation.ValidationError;
import com.netflix.conductor.core.utils.Utils;
import com.netflix.conductor.metrics.Monitors;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.ValidationException;

/** This class converts Hibernate {@link ValidationException} into http response. */
@RestControllerAdvice
@Order(ValidationExceptionMapper.ORDER)
public class ValidationExceptionMapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationExceptionMapper.class);

    public static final int ORDER = Ordered.HIGHEST_PRECEDENCE;

    private final String host = Utils.getServerId();

    @ExceptionHandler(ValidationException.class)
    public ResponseEntity<ErrorResponse> toResponse(
            HttpServletRequest request, ValidationException exception) {
        logException(request, exception);

        HttpStatus httpStatus;

        if (exception instanceof ConstraintViolationException) {
            httpStatus = HttpStatus.BAD_REQUEST;
        } else {
            httpStatus = HttpStatus.INTERNAL_SERVER_ERROR;
            Monitors.error("error", "error");
        }

        return new ResponseEntity<>(toErrorResponse(exception), httpStatus);
    }

    private ErrorResponse toErrorResponse(ValidationException ve) {
        if (ve instanceof ConstraintViolationException exception) {
            return constraintViolationExceptionToErrorResponse(exception);
        } else {
            ErrorResponse result = new ErrorResponse();
            result.setStatus(HttpStatus.INTERNAL_SERVER_ERROR.value());
            result.setMessage(ve.getMessage());
            result.setInstance(host);
            return result;
        }
    }

    private ErrorResponse constraintViolationExceptionToErrorResponse(
            ConstraintViolationException exception) {
        ErrorResponse errorResponse = new ErrorResponse();
        errorResponse.setStatus(HttpStatus.BAD_REQUEST.value());
        errorResponse.setMessage("Validation failed, check below errors for detail.");

        List<ValidationError> validationErrors = new ArrayList<>();

        exception
                .getConstraintViolations()
                .forEach(
                        e ->
                                validationErrors.add(
                                        new ValidationError(
                                                getViolationPath(e),
                                                e.getMessage(),
                                                getViolationInvalidValue(e.getInvalidValue()))));

        errorResponse.setValidationErrors(validationErrors);
        return errorResponse;
    }

    private String getViolationPath(final ConstraintViolation<?> violation) {
        final String propertyPath = violation.getPropertyPath().toString();
        return !"".equals(propertyPath) ? propertyPath : "";
    }

    private String getViolationInvalidValue(final Object invalidValue) {
        if (invalidValue == null) {
            return null;
        }

        if (invalidValue.getClass().isArray()) {
            if (invalidValue instanceof Object[]) {
                // not helpful to return object array, skip it.
                return null;
            } else if (invalidValue instanceof boolean[] booleans) {
                return Arrays.toString(booleans);
            } else if (invalidValue instanceof byte[] bytes) {
                return Arrays.toString(bytes);
            } else if (invalidValue instanceof char[] chars) {
                return Arrays.toString(chars);
            } else if (invalidValue instanceof double[] doubles) {
                return Arrays.toString(doubles);
            } else if (invalidValue instanceof float[] floats) {
                return Arrays.toString(floats);
            } else if (invalidValue instanceof int[] ints) {
                return Arrays.toString(ints);
            } else if (invalidValue instanceof long[] longs) {
                return Arrays.toString(longs);
            } else if (invalidValue instanceof short[] shorts) {
                return Arrays.toString(shorts);
            }
        }

        // It is only helpful to return invalid value of primitive types
        if (invalidValue.getClass().getName().startsWith("java.lang.")) {
            return invalidValue.toString();
        }

        return null;
    }

    private void logException(HttpServletRequest request, ValidationException exception) {
        LOGGER.error(
                "Error {} url: '{}'",
                exception.getClass().getSimpleName(),
                request.getRequestURI(),
                exception);
    }
}
