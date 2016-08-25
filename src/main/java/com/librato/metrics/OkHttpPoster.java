package com.librato.metrics;

import static com.librato.metrics.Authorization.buildAuthHeader;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;

public class OkHttpPoster implements HttpPoster {
  private static final Logger LOG = LoggerFactory.getLogger(OkHttpPoster.class);
  public static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
  private final URL url;
  private final String auth;
  private OkHttpClient client;

  public OkHttpPoster(String url, String email, String token) {
    this.auth = buildAuthHeader(email, token);
    try {
      this.url = new URL(url);
    } catch (MalformedURLException e) {
      throw new RuntimeException("Could not parse URL", e);
    }
    this.client = new OkHttpClient();
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public Future<Response> post(String userAgent, String payload) throws IOException {

    RequestBody body = RequestBody.create(JSON, payload);
    Request request = new Request.Builder().url(this.url).addHeader("Authorization", this.auth)
        .addHeader("User-Agent", "Abacus-Library").post(body).build();
    final FutureResponse result = new FutureResponse();
    LOG.info("Making http call using OkHttpClient");
    client.newCall(request).enqueue(new Callback() {

      @Override
      public void onFailure(Call call, IOException e) {
        LOG.error("Error making http call.", e);
        result.setException(new CouldNotPostMeasurementsException(e));
      }

      @Override
      public void onResponse(Call call, final okhttp3.Response resp) throws IOException {
        LOG.info("HTTP Response received: {}", resp);
        result.set(new Response() {

          @Override
          public int getStatusCode() {
            return resp.code();
          }

          @Override
          public String getBody() throws IOException {
            return resp.body().string();
          }
        });
      }
    });
    return result;
  }

  class CouldNotPostMeasurementsException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public CouldNotPostMeasurementsException(Throwable cause) {
      super("Could not post measures to " + url, cause);
    }
  }

  class FutureResponse implements Future<Response> {
    private final CountDownLatch latch = new CountDownLatch(1);
    private Response response = null;
    private RuntimeException exception = null;

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return true;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return this.response != null || this.exception != null;
    }

    @Override
    public Response get() throws InterruptedException, ExecutionException {
      this.latch.await();
      return this.throwOrGet();
    }

    @Override
    public Response get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      this.latch.await(timeout, unit);
      return this.throwOrGet();
    }

    private Response throwOrGet() {
      if (this.exception != null) {
        throw this.exception;
      } else {
        return this.response;
      }
    }

    public void set(Response response) {
      this.response = response;
      this.latch.countDown();
    }

    public void setException(RuntimeException e) {
      this.exception = e;
      this.latch.countDown();
    }
  }
}
