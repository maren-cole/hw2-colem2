import json
from flask import Flask, request, jsonify
from google.cloud import datastore

app = Flask(__name__)
client = datastore.Client()


@app.route('/businesses', methods=['POST'])
def create_business():
    """Create a new business"""
    content = request.get_json()

    # Required attributes
    required = ['owner_id', 'name', 'street_address', 'city', 'state', 'zip_code']
    if not all(field in content for field in required):
        return jsonify({"Error": "The request body is missing at least one of the required attributes"}), 400

    # Create
    key = client.key('Business')
    business = datastore.Entity(key=key)
    business.update({
        'owner_id': content['owner_id'],
        'name': content['name'],
        'street_address': content['street_address'],
        'city': content['city'],
        'state': content['state'],
        'zip_code': content['zip_code']
    })
    client.put(business)

    # Return created business with id
    response = {
        'id': business.key.id,
        'owner_id': business['owner_id'],
        'name': business['name'],
        'street_address': business['street_address'],
        'city': business['city'],
        'state': business['state'],
        'zip_code': business['zip_code']
    }
    return jsonify(response), 201


@app.route('/businesses/<int:business_id>', methods=['GET'])
def get_business(business_id):
    """Get a specific business"""
    key = client.key('Business', business_id)
    business = client.get(key)

    if not business:
        return jsonify({"Error": "No business with this business_id exists"}), 404

    response = {
        'id': business.key.id,
        'owner_id': business['owner_id'],
        'name': business['name'],
        'street_address': business['street_address'],
        'city': business['city'],
        'state': business['state'],
        'zip_code': business['zip_code']
    }
    return jsonify(response), 200


@app.route('/businesses', methods=['GET'])
def list_businesses():
    """List all businesses"""
    query = client.query(kind='Business')
    results = list(query.fetch())

    businesses = []
    for business in results:
        businesses.append({
            'id': business.key.id,
            'owner_id': business['owner_id'],
            'name': business['name'],
            'street_address': business['street_address'],
            'city': business['city'],
            'state': business['state'],
            'zip_code': business['zip_code']
        })

    return jsonify(businesses), 200


@app.route('/businesses/<int:business_id>', methods=['PUT'])
def edit_business(business_id):
    """Edit a business (replacement semantics)"""
    content = request.get_json()

    # Required attributes
    required = ['owner_id', 'name', 'street_address', 'city', 'state', 'zip_code']
    if not all(field in content for field in required):
        return jsonify({"Error": "The request body is missing at least one of the required attributes"}), 400

    # Check for duplicate
    key = client.key('Business', business_id)
    business = client.get(key)

    if not business:
        return jsonify({"Error": "No business with this business_id exists"}), 404

    # Update business
    business.update({
        'owner_id': content['owner_id'],
        'name': content['name'],
        'street_address': content['street_address'],
        'city': content['city'],
        'state': content['state'],
        'zip_code': content['zip_code']
    })
    client.put(business)

    response = {
        'id': business.key.id,
        'owner_id': business['owner_id'],
        'name': business['name'],
        'street_address': business['street_address'],
        'city': business['city'],
        'state': business['state'],
        'zip_code': business['zip_code']
    }
    return jsonify(response), 200


@app.route('/businesses/<int:business_id>', methods=['DELETE'])
def delete_business(business_id):
    """Delete a business and all its reviews"""
    key = client.key('Business', business_id)
    business = client.get(key)

    if not business:
        return jsonify({"Error": "No business with this business_id exists"}), 404

    # Delete reviews
    query = client.query(kind='Review')
    query.add_filter('business_id', '=', business_id)
    reviews = list(query.fetch())

    for review in reviews:
        client.delete(review.key)

    # Delete
    client.delete(key)

    return '', 204


@app.route('/owners/<int:owner_id>/businesses', methods=['GET'])
def list_owner_businesses(owner_id):
    """List all businesses for a specific owner"""
    query = client.query(kind='Business')
    query.add_filter('owner_id', '=', owner_id)
    results = list(query.fetch())

    businesses = []
    for business in results:
        businesses.append({
            'id': business.key.id,
            'owner_id': business['owner_id'],
            'name': business['name'],
            'street_address': business['street_address'],
            'city': business['city'],
            'state': business['state'],
            'zip_code': business['zip_code']
        })

    return jsonify(businesses), 200


# Review endpoints

@app.route('/reviews', methods=['POST'])
def create_review():
    """Create a new review"""
    content = request.get_json()

    # Required
    required = ['user_id', 'business_id', 'stars']
    if not all(field in content for field in required):
        return jsonify({"Error": "The request body is missing at least one of the required attributes"}), 400

    # Check for duplicate
    business_key = client.key('Business', content['business_id'])
    business = client.get(business_key)

    if not business:
        return jsonify({"Error": "No business with this business_id exists"}), 404

    # Check if user already reviewed
    query = client.query(kind='Review')
    query.add_filter('user_id', '=', content['user_id'])
    query.add_filter('business_id', '=', content['business_id'])
    existing_reviews = list(query.fetch())

    if existing_reviews:
        return jsonify({"Error": "You have already submitted a review for this business. You can update your previous review, or delete it and submit a new review"}), 409

    # Create new review
    key = client.key('Review')
    review = datastore.Entity(key=key)
    review.update({
        'user_id': content['user_id'],
        'business_id': content['business_id'],
        'stars': content['stars']
    })

    # Add review text
    if 'review_text' in content:
        review['review_text'] = content['review_text']

    client.put(review)

    # Return created review with id
    response = {
        'id': review.key.id,
        'user_id': review['user_id'],
        'business_id': review['business_id'],
        'stars': review['stars']
    }

    if 'review_text' in review:
        response['review_text'] = review['review_text']

    return jsonify(response), 201


@app.route('/reviews/<int:review_id>', methods=['GET'])
def get_review(review_id):
    """Get a specific review"""
    key = client.key('Review', review_id)
    review = client.get(key)

    if not review:
        return jsonify({"Error": "No review with this review_id exists"}), 404

    response = {
        'id': review.key.id,
        'user_id': review['user_id'],
        'business_id': review['business_id'],
        'stars': review['stars']
    }

    if 'review_text' in review:
        response['review_text'] = review['review_text']

    return jsonify(response), 200


@app.route('/reviews/<int:review_id>', methods=['PUT'])
def edit_review(review_id):
    """Edit a review (partial update semantics)"""
    content = request.get_json()

    # Required
    if 'stars' not in content:
        return jsonify({"Error": "The request body is missing at least one of the required attributes"}), 400

    # Check if review exists
    key = client.key('Review', review_id)
    review = client.get(key)

    if not review:
        return jsonify({"Error": "No review with this review_id exists"}), 404

    # Update stars
    review['stars'] = content['stars']

    # Update review_text
    if 'review_text' in content:
        review['review_text'] = content['review_text']

    client.put(review)

    response = {
        'id': review.key.id,
        'user_id': review['user_id'],
        'business_id': review['business_id'],
        'stars': review['stars']
    }

    if 'review_text' in review:
        response['review_text'] = review['review_text']

    return jsonify(response), 200


@app.route('/reviews/<int:review_id>', methods=['DELETE'])
def delete_review(review_id):
    """Delete a review"""
    key = client.key('Review', review_id)
    review = client.get(key)

    if not review:
        return jsonify({"Error": "No review with this review_id exists"}), 404

    client.delete(key)

    return '', 204


@app.route('/users/<int:user_id>/reviews', methods=['GET'])
def list_user_reviews(user_id):
    """List all reviews for a specific user"""
    query = client.query(kind='Review')
    query.add_filter('user_id', '=', user_id)
    results = list(query.fetch())

    reviews = []
    for review in results:
        review_data = {
            'id': review.key.id,
            'user_id': review['user_id'],
            'business_id': review['business_id'],
            'stars': review['stars']
        }

        if 'review_text' in review:
            review_data['review_text'] = review['review_text']

        reviews.append(review_data)

    return jsonify(reviews), 200


if __name__ == "__main__":
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    # Flask's development server will automatically serve static files in
    # the "static" directory. See:
    # http://flask.pocoo.org/docs/1.0/quickstart/#static-files. Once deployed,
    # App Engine itself will serve those files as configured in app.yaml.
    app.run(host="127.0.0.1", port=8080, debug=True)
