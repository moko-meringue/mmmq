package org.mmmq.gateway.fixture;

import org.springframework.boot.test.web.client.MockServerRestClientCustomizer;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

public class MockRestServiceServerFixture {

    private final MockRestServiceServer mockServer;
    private final RestClient restClient;

    private MockRestServiceServerFixture(MockRestServiceServer mockServer, RestClient restClient) {
        this.mockServer = mockServer;
        this.restClient = restClient;
    }

    public static MockRestServiceServerFixture create(RestClient restClient) {
        RestClient.Builder builder = restClient.mutate();

        MockServerRestClientCustomizer customizer = new MockServerRestClientCustomizer();
        customizer.customize(builder);
        MockRestServiceServer mockServer = customizer.getServer();

        return new MockRestServiceServerFixture(mockServer, builder.build());
    }

    public MockRestServiceServer getMockServer() {
        return mockServer;
    }

    public RestClient getRestClient() {
        return restClient;
    }
}
