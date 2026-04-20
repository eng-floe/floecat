---
title: Blog
layout: single
permalink: /blog/
author_profile: false
classes: wide
---

{% assign featured = site.posts | first %}
{% if featured %}
<section class="featured-post">
  <p class="featured-post__label">Featured</p>
  <h2><a href="{{ featured.url | relative_url }}">{{ featured.title }}</a></h2>
  <p class="featured-post__meta">
    {{ featured.date | date: "%Y-%m-%d" }}
    {% assign featured_words = featured.content | strip_html | number_of_words %}
    {% assign featured_read = featured_words | divided_by: 180 | plus: 1 %}
    . {{ featured_read }} min read
  </p>
  <p>{{ featured.excerpt | strip_html | truncate: 260 }}</p>
  <a class="btn btn--primary" href="{{ featured.url | relative_url }}">Read Post</a>
</section>
{% endif %}

<section class="blog-grid">
  {% for post in site.posts %}
    <article class="post-card">
      <h3><a href="{{ post.url | relative_url }}">{{ post.title }}</a></h3>
      <p class="post-card__meta">
        {{ post.date | date: "%Y-%m-%d" }}
        {% assign words = post.content | strip_html | number_of_words %}
        {% assign read = words | divided_by: 180 | plus: 1 %}
        . {{ read }} min read
      </p>
      <p>{{ post.excerpt | strip_html | truncate: 180 }}</p>
      {% if post.tags and post.tags.size > 0 %}
        <div class="post-tags">
          {% for tag in post.tags %}
            <span class="post-tag">{{ tag }}</span>
          {% endfor %}
        </div>
      {% endif %}
      <a href="{{ post.url | relative_url }}">Read article</a>
    </article>
  {% endfor %}
</section>
