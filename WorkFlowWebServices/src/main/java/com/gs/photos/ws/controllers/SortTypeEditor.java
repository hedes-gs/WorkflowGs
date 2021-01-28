package com.gs.photos.ws.controllers;

import java.beans.PropertyEditorSupport;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.domain.Sort.Order;
import org.springframework.util.StringUtils;

public class SortTypeEditor extends PropertyEditorSupport {

    protected Sort parseParameterIntoSort(List<String> source, String delimiter) {

        List<Order> allOrders = new ArrayList<>();

        for (String part : source) {
            if (part == null) {
                continue;
            }

            String[] elements = Arrays.stream(part.split(delimiter))
                .filter((e) -> StringUtils.hasText(e.replace(".", "")))
                .toArray(String[]::new);

            Optional<Direction> direction = elements.length == 0 ? Optional.empty()
                : Direction.fromOptionalString(elements[elements.length - 1]);

            int lastIndex = direction.map(it -> elements.length - 1)
                .orElseGet(() -> elements.length);

            for (int i = 0; i < lastIndex; i++) {
                SortTypeEditor.toOrder(elements[i], direction)
                    .ifPresent(allOrders::add);
            }
        }

        return allOrders.isEmpty() ? Sort.unsorted() : Sort.by(allOrders);

    }

    private static Optional<Order> toOrder(String property, Optional<Direction> direction) {

        if (!StringUtils.hasText(property)) { return Optional.empty(); }

        return Optional.of(
            direction.map(it -> new Order(it, property))
                .orElseGet(() -> Order.by(property)));
    }

    @Override
    public void setAsText(String text) throws IllegalArgumentException {
        // sorted,true,unsorted,true,empty,true
        Sort sort = this.parseParameterIntoSort(Arrays.asList(text), ",");
        this.setValue(sort);
    }
}
