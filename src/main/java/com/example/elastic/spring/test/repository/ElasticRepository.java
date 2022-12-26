package com.example.elastic.spring.test.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.indices.IndexSettings;
import co.elastic.clients.json.JsonData;
import com.example.elastic.spring.test.domain.Product;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import java.util.stream.Collectors;

@Repository
@AllArgsConstructor
public class ElasticRepository {

    private ElasticsearchClient client;

    public List<Product> search(String idx_name, String description) {
        try {
            SearchResponse<Product> search = client.search(s -> s
                            .index(idx_name)
                            .query(q -> q.match(t -> t.field("description").query(description))),
                    Product.class);
            return search.hits().hits().stream().map(Hit::source).collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Product> searchAll(String idx_name) {
        try {
            SearchResponse<Product> search = client.search(s -> s
                            .index(idx_name)
                            .query(q -> q.matchAll(builder -> builder)),
                    Product.class);

            return search.hits().hits().stream().map(Hit::source).collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Product> searchRange(String idx_name, Integer price) {
        try {
            SearchResponse<Product> search = client.search(s -> s
                            .index(idx_name)
                            .query(q -> q.range(builder -> builder.field("price").gte(JsonData.of(price)))),
                    Product.class);

            /**
             gte - Greater-than or equal to

             lte - Less-than or equal to

             gt - Greater-than

             lt - Less-than
             **/

            return search.hits().hits().stream().map(Hit::source).collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void createIndex(String idx_name) {
        try {
            IndexSettings settings = new IndexSettings.Builder()
                    .numberOfShards("2")
                    .numberOfReplicas("2").build();
            client.indices().create(c -> c.settings(settings).index(idx_name));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public void delete(String index_name) {
        try {
            client.indices().delete(c -> c.index(index_name));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public void populate(String idx_name, Product prod) {
        try {
            IndexResponse response = client.index(i -> i
                    .index(idx_name)
                    //If you don't specify this field, Elastic will create it automatically
                    .id(prod.getId())
                    .document(prod)
            );
            System.out.println(response.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public void populateRaw() {
        try {
            Reader input = new StringReader("{'@timestamp': '2022-04-08T13:55:32Z', 'level': 'warn', 'message': 'Some log message'}"
                    .replace('\'', '"'));

            IndexRequest<JsonData> request = IndexRequest.of(i -> i
                    .index("logs")
                    .withJson(input)
            );

            IndexResponse response = client.index(request);

            System.out.println(response.version());
        } catch (ElasticsearchException e) {
            System.err.println(e.getMessage());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public Product searchById(String index_name, String id_document) {
        try {
            GetResponse<Product> response = client.get(g -> g
                            .index(index_name)
                            .id(id_document),
                    Product.class
            );

            if (response.found()) {
                return response.source();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public void searchByIdRaw() {
        try {
            GetResponse<ObjectNode> response = client.get(g -> g
                            .index("products")
                            .id("12345"),
                    ObjectNode.class
            );

            if (response.found()) {
                ObjectNode json = response.source();
                String name = json.get("description").asText();
                System.out.println("Product is " + name);
            } else {
                System.out.println("Product not found");
            }
        } catch (ElasticsearchException e) {
            System.err.println(e.getMessage());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void updateDocument(Product prod, String index_name, String document_id) {
        try {
            //With the update we can change the current fields or add a new ones
            client.update(u -> u.index(index_name)
                    .id(document_id)
                    .doc(prod), Product.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteDocument() {
        try {
            client.delete(u -> u.index("products").id("12345"));
        } catch (ElasticsearchException e) {
            System.err.println(e.getMessage());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void upsertDocument() {
        try {
            Product prod = new Product("bk-1", "City bike", 999);
            client.update(u -> u.index("products")
                    .id("12345")
                    .upsert(prod), Product.class);
        } catch (ElasticsearchException e) {
            System.err.println(e.getMessage());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void scriptingDocument() {
        try {
            //With the update we can change the current fields or add a new ones
            client.update(u -> u.index("products")
                    .id("12345")
                    .script(builder -> builder.inline(builder1 -> builder1.source(""))), Product.class);
        } catch (ElasticsearchException e) {
            System.err.println(e.getMessage());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void scripting() {
        try {
            Product prod = new Product("bk-1", "City bike", 999);
            client.putScript(builder -> builder.script(builder1 -> builder1.source("")));
        } catch (ElasticsearchException e) {
            System.err.println(e.getMessage());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
