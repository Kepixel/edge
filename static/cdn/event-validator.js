(function() {
    // Event validation schema based on ecommerce-json-schema.json
    const eventSchemas = {
        'Products Searched': {
            required: ['event', 'query']
        },
        'Product List Viewed': {
            required: ['event', 'products']
        },
        'Product List Filtered': {
            required: ['event', 'products']
        },
        'Coupon Entered': {
            required: ['event', 'coupon_id']
        },
        'Coupon Applied': {
            required: ['event', 'coupon_id', 'coupon_name', 'discount']
        },
        'Coupon Denied': {
            required: ['event', 'coupon_id', 'reason']
        },
        'Coupon Removed': {
            required: ['event', 'coupon_id', 'coupon_name', 'discount']
        },
        'Product Clicked': {
            required: ['event', 'product_id', 'name', 'price', 'category']
        },
        'Product Viewed': {
            required: ['event', 'product_id', 'name', 'price', 'category']
        },
        'Product Added': {
            required: ['event', 'cart_id', 'product_id', 'name', 'price', 'category']
        },
        'Product Removed': {
            required: ['event', 'cart_id', 'product_id', 'name', 'price', 'category']
        },
        'Cart Viewed': {
            required: ['event', 'cart_id', 'products']
        },
        'Checkout Started': {
            required: ['event', 'order_id', 'products']
        },
        'Checkout Step Viewed': {
            required: ['event', 'checkout_id', 'step']
        },
        'Checkout Step Completed': {
            required: ['event', 'checkout_id', 'step']
        },
        'Payment Info Entered': {
            required: ['event', 'checkout_id']
        },
        'Order Updated': {
            required: ['event', 'order_id', 'products']
        },
        'Order Completed': {
            required: ['event', 'order_id', 'products']
        },
        'Order Refunded': {
            required: ['event', 'order_id', 'products']
        },
        'Order Cancelled': {
            required: ['event', 'order_id', 'products']
        },
        'Product Reviewed': {
            required: ['event', 'product_id', 'review_id', 'review_body', 'rating']
        },
        'Product Shared': {
            required: ['event', 'share_via', 'recipient', 'product_id', 'name', 'category']
        },
        'Cart Shared': {
            required: ['event', 'share_via', 'recipient', 'cart_id', 'products']
        },
        'Product Added to Wishlist': {
            required: ['event', 'wishlist_id', 'product_id', 'name', 'category']
        },
        'Product Removed from Wishlist': {
            required: ['event', 'wishlist_id', 'product_id', 'name', 'category']
        },
        'Wishlist Product Added to Cart': {
            required: ['event', 'wishlist_id', 'product_id', 'name', 'category']
        },
        // Marketing events for zid.sa
        'Link Clicked': {
            required: ['event', 'link_url']
        },
        'Form Started': {
            required: ['event']
        },
        'Form Viewed': {
            required: ['event']
        }
    };

    /**
     * Validates if a value is not null, undefined, or empty string
     * @param {*} value - The value to check
     * @returns {boolean} - True if value is valid, false otherwise
     */
    function isValidValue(value) {
        return value !== null && value !== undefined && value !== '';
    }

    /**
     * Validates an array of products based on the schema requirements
     * @param {Array} products - Array of product objects
     * @returns {Object} - Validation result with isValid boolean and errors array
     */
    function validateProducts(products) {
        if (!Array.isArray(products) || products.length === 0) {
            return {
                isValid: false,
                errors: ['products must be a non-empty array']
            };
        }

        const errors = [];
        const requiredProductFields = ['product_id', 'name', 'price', 'category'];

        products.forEach((product, index) => {
            if (typeof product !== 'object' || product === null) {
                errors.push(`products[${index}] must be an object`);
                return;
            }

            requiredProductFields.forEach(field => {
                if (!isValidValue(product[field])) {
                    errors.push(`products[${index}].${field} is required`);
                }
            });
        });

        return {
            isValid: errors.length === 0,
            errors: errors
        };
    }

    /**
     * Validates an event payload against the schema requirements
     * @param {string} eventName - Name of the event
     * @param {Object} payload - Event payload to validate
     * @returns {Object} - Validation result with isValid boolean and errors array
     */
    function validateEventPayload(eventName, payload) {
        const schema = eventSchemas[eventName];

        if (!schema) {
            return {
                isValid: true, // Allow events not in schema to pass through
                errors: []
            };
        }

        const errors = [];
        const combinedPayload = Object.assign({}, payload, { event: eventName });

        // Check required fields
        schema.required.forEach(field => {
            if (field === 'products') {
                // Special handling for products array
                const productValidation = validateProducts(combinedPayload[field]);
                if (!productValidation.isValid) {
                    errors.push(...productValidation.errors);
                }
            } else if (!isValidValue(combinedPayload[field])) {
                errors.push(`${field} is required for event "${eventName}"`);
            }
        });

        // Validate numeric fields
        const numericFields = ['price', 'discount', 'step', 'rating', 'quantity'];
        numericFields.forEach(field => {
            if (combinedPayload[field] !== undefined && combinedPayload[field] !== null) {
                const value = Number(combinedPayload[field]);
                if (isNaN(value) || value < 0) {
                    errors.push(`${field} must be a valid positive number`);
                }
            }
        });

        // Validate rating range if present
        if (combinedPayload.rating !== undefined && combinedPayload.rating !== null) {
            const rating = Number(combinedPayload.rating);
            if (!isNaN(rating) && (rating < 1 || rating > 5)) {
                errors.push('rating must be between 1 and 5');
            }
        }

        return {
            isValid: errors.length === 0,
            errors: errors
        };
    }

    // Expose validation function globally
    window.kepixelEventValidator = {
        validateEventPayload: validateEventPayload,
        eventSchemas: eventSchemas
    };
})();
