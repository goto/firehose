package com.gotocompany.firehose.utils;

import com.google.protobuf.Message;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;
import org.aeonbits.owner.util.Collections;

public class CelUtils {

    public static Object evaluate(CelRuntime.Program program, Message payload) {
        try {
            return program.eval(Collections.map(payload.getDescriptorForType().getFullName(), payload));
        } catch (CelEvaluationException e) {
            throw new IllegalArgumentException("Could not evaluate Cel expression", e);
        }
    }

}
