package com.example.elastic.spring.test.controller;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.example.elastic.spring.test.domain.Product;
import com.example.elastic.spring.test.repository.ElasticRepository;
import lombok.AllArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@AllArgsConstructor
public class ElasticController {

    private ElasticRepository repository;


    @PostMapping("/index")
    public ResponseEntity<Object> index(@RequestParam("idx_name") String idx_name) {
        try {
            repository.createIndex(idx_name);
            return ResponseEntity.accepted().build();
        } catch (ElasticsearchException ex) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body("Index already exists.");
        }
    }

    @DeleteMapping("/index/{idx_name}")
    public ResponseEntity<Object> deleteIndexAll(@PathVariable("idx_name") String idx_name) {
        try {
            repository.delete(idx_name);
            return ResponseEntity.ok().build();
        } catch (ElasticsearchException ex) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ex.getMessage());
        }
    }

    @GetMapping("/documents/{idx_name}")
    public ResponseEntity<Object> indexAll(@PathVariable("idx_name") String idx_name,
                                           @RequestParam(value = "desc", required = false) String desc,
                                           @RequestParam(value = "price", required = false) String price) {
        try {
            if (desc != null && !desc.isBlank()) {
                return ResponseEntity.ok(repository.search(idx_name, desc));
            }
            if (price != null && !price.isBlank()) {
                return ResponseEntity.ok(repository.searchRange(idx_name, Integer.valueOf(price)));
            }
            return ResponseEntity.ok(repository.searchAll(idx_name));
        } catch (ElasticsearchException ex) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ex.getMessage());
        }
    }

    @PostMapping("/document/{idx_name}")
    public ResponseEntity<Object> document(@PathVariable("idx_name") String idx_name, @RequestBody Product product) {
        try {
            repository.populate(idx_name, product);
            return ResponseEntity.accepted().build();
        } catch (ElasticsearchException ex) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ex.getMessage());
        }
    }

    @PutMapping("/document/{idx_name}/{id_document}")
    public ResponseEntity<Object> updateDocument(@PathVariable("idx_name") String idx_name,
                                                 @PathVariable("id_document") String id_document,
                                                 @RequestBody Product product) {
        try {
            repository.updateDocument(product, idx_name, id_document);
            return ResponseEntity.ok().build();
        } catch (ElasticsearchException ex) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ex.getMessage());
        }
    }

    @GetMapping("/document/{idx_name}/{id_document}")
    public ResponseEntity<Object> documentById(@PathVariable("idx_name") String idx_name,
                                               @PathVariable("id_document") String id_document) {
        try {
            Product product = repository.searchById(idx_name, id_document);
            if (product == null) {
                return ResponseEntity.notFound().build();
            }
            return ResponseEntity.ok(product);
        } catch (ElasticsearchException ex) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ex.getMessage());
        }
    }

}
