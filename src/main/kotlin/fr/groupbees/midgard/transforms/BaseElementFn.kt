package fr.groupbees.midgard.transforms

import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.TypeDescriptor

/**
 * Base class for the custom DoFn allowing the interaction with lifecycle methods.
 *
 * This class gives shared elements like type descriptors, input and output
 *
 * The class that extend [fr.groupbees.midgard.transforms.BaseElementFn] must give input and output type descriptors and
 * uses the tuple tags to handle good output and failure.
 *
 * @param <InputT>  input of [org.apache.beam.sdk.transforms.DoFn]
 * @param <OutputT> output of [org.apache.beam.sdk.transforms.DoFn]
 * @author mazlum
 */
abstract class BaseElementFn<InputT, OutputT> : DoFn<InputT, OutputT> {

    @JvmField
    @Transient
    protected var inputType: TypeDescriptor<InputT>

    @JvmField
    @Transient
    protected var outputType: TypeDescriptor<OutputT>

    /**
     *
     * Constructor for BaseElementFn.
     */
    protected constructor() {
        inputType = super.getInputTypeDescriptor()
        outputType = super.getOutputTypeDescriptor()
    }

    /**
     * Constructor for BaseElementFn.
     *
     * @param inputType  a [org.apache.beam.sdk.values.TypeDescriptor] object
     * @param outputType a [org.apache.beam.sdk.values.TypeDescriptor] object
     */
    protected constructor(inputType: TypeDescriptor<InputT>, outputType: TypeDescriptor<OutputT>) {
        this.inputType = inputType
        this.outputType = outputType
    }

    /**
     *
     * Getter for the field `inputType`.
     *
     * @return a [org.apache.beam.sdk.values.TypeDescriptor] object
     */
    override fun getInputTypeDescriptor(): TypeDescriptor<InputT> {
        return inputType
    }

    /**
     *
     * Getter for the field `outputType`.
     *
     * @return a [org.apache.beam.sdk.values.TypeDescriptor] object
     */
    override fun getOutputTypeDescriptor(): TypeDescriptor<OutputT> {
        return outputType
    }
}