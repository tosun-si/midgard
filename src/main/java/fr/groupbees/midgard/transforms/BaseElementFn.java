package fr.groupbees.midgard.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Base class for the custom DoFn allowing the interaction with lifecycle methods.
 * <p>
 * This class gives shared elements like type descriptors, input and output
 *
 * <p>
 * The class that extend {@link fr.groupbees.midgard.transforms.BaseElementFn} must give input and output type descriptors and
 * uses the tuple tags to handle good output and failure.
 * </p>
 *
 * @param <InputT>  input of {@link org.apache.beam.sdk.transforms.DoFn}
 * @param <OutputT> output of {@link org.apache.beam.sdk.transforms.DoFn}
 * @author mazlum
 */
public abstract class BaseElementFn<InputT, OutputT> extends DoFn<InputT, OutputT> {

    protected transient TypeDescriptor<InputT> inputType;
    protected transient TypeDescriptor<OutputT> outputType;

    /**
     * <p>Constructor for BaseElementFn.</p>
     */
    protected BaseElementFn() {
        inputType = super.getInputTypeDescriptor();
        outputType = super.getOutputTypeDescriptor();
    }

    /**
     * <p>Constructor for BaseElementFn.</p>
     *
     * @param inputType  a {@link org.apache.beam.sdk.values.TypeDescriptor} object
     * @param outputType a {@link org.apache.beam.sdk.values.TypeDescriptor} object
     */
    protected BaseElementFn(TypeDescriptor<InputT> inputType, TypeDescriptor<OutputT> outputType) {
        this.inputType = inputType;
        this.outputType = outputType;
    }

    /**
     * <p>Getter for the field <code>inputType</code>.</p>
     *
     * @return a {@link org.apache.beam.sdk.values.TypeDescriptor} object
     */
    public TypeDescriptor<InputT> getInputTypeDescriptor() {
        return inputType;
    }

    /**
     * <p>Getter for the field <code>outputType</code>.</p>
     *
     * @return a {@link org.apache.beam.sdk.values.TypeDescriptor} object
     */
    public TypeDescriptor<OutputT> getOutputTypeDescriptor() {
        return outputType;
    }
}
