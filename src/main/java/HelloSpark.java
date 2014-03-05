/**
 * Created by Asra Nizami on 3/3/14.
 */

import static spark.Spark.*;
import spark.*;
import java.util.ArrayList;

import spark.Request;
import spark.Response;
import spark.Route;

import java.util.ArrayDeque;
import java.util.Deque;

public class HelloSpark {

    public static ArticleDbService<Article> articleDBservice = new ArticlePostgresDAO<Article>();

//    public static Deque<Article> articles = new ArrayDeque<Article>();


    public static void main(String[] args) {

        get(new Route("/") {
            @Override
            public Object handle(Request request, Response response) {

                ArrayList<Article> articles = articleDBservice.readAll();

                String title = "My Blog";
                String createArticleLink = "<a href='/article/create'>Write Article</a>";
                StringBuilder html = new StringBuilder();
                html.append("<h1>").append(title).append("</h1>").append(createArticleLink);
                html.append("<hr>");
                if(articles.isEmpty()) {
                    html.append("<b>No articles have been posted. This is a test!</b>");
                } else {
                    for(Article article : articles) {
                        if(article.readable()) {
                            html.append("Title: ").append(article.getTitle())
                                    .append("<br/>")
                                    .append(article.getCreatedAt())
                                    .append("<br/>")
                                    .append("Summary: ").append(article.getSummaryLink())
                                    .append("<br/>")
                                    .append(article.getEditLink()).append(" | ").append(article.getDeleteLink())
                                    .append("</p>");
                        }
                    }
                }
                return html.toString();
            }
        });


        get(new Route("/article/create") {
            @Override
            public Object handle(Request request, Response response) {
                StringBuilder form = new StringBuilder();

                form.append("<form id='article-create-form' method='POST' action='/article/create'>")
                        .append("Title: <input type='text' name='article-title' />")
                        .append("<br/>")
                        .append("Summary: <input type='text' name='article-summary' />")
                        .append("<br/>")
                        .append("</form>")
                        .append("<textarea name='article-content' rows='4' cols='50' form='article-create-form'></textarea>")
                        .append("<br/>")
                        .append("<input type='submit' value='Publish' form='article-create-form' />");

                return form.toString();
            }
        });

        post(new Route("/article/create") {
            @Override
            public Object handle(Request request, Response response) {
                String title = request.queryParams("article-title");
                String summary = request.queryParams("article-summary");
                String content = request.queryParams("article-content");

                Article article = new Article(title, summary, content, articleDBservice.readAll().size());
                articleDBservice.create(article);
//                HelloSpark.articles.addFirst(article);
                response.status(201);
                response.redirect("/");
                System.out.println("Trying to create article");
                return "";
            }
        });

        get(new Route("/article/read/:id") {
            @Override
            public Object handle(Request request, Response response) {
                Integer id = Integer.parseInt(request.params(":id"));
                StringBuilder html = new StringBuilder();

                ArrayList<Article> articles = articleDBservice.readAll();

                for(Article article : articles) {
                    if(id.equals(article.getId())) {
                        html.append("<a href='/'>Home</a>").append("<p />")
                                .append("Title: ").append(article.getTitle()).append("<br />")
                                .append(article.getCreatedAt())
                                .append("<p>").append(article.getContent()).append("</p>");
                        break;
                    }
                }
                return html.toString();
            }
        });

        get(new Route("/article/update/:id") {
            @Override
            public Object handle(Request request, Response response) {
                Integer id = Integer.parseInt(request.params(":id"));
                StringBuilder form = new StringBuilder();

                ArrayList<Article> articles = articleDBservice.readAll();

                for(Article article : articles) {
                    if(id.equals(article.getId())) {
                        form.append("<form id='article-create-form' method='POST' action='/article/update/:id'>")
                                .append("Title: <input type='text' name='article-title' value='").append(article.getTitle()).append("' />")
                                .append("<br/>")
                                .append("Summary: <input type='text' name='article-summary' value='").append(article.getSummary()).append("' />")
                                .append("<input type='hidden' name='article-id' value='").append(article.getId().toString()).append("' />")
                                .append("<br/>")
                                .append("</form>")
                                .append("<textarea name='article-content' rows='4' cols='50' form='article-create-form'>").append(article.getContent())
                                .append("</textarea>")
                                .append("<br/>")
                                .append("<input type='submit' value='Update' form='article-create-form' />");
                        break;
                    }
                }

                return form.toString();
            }
        });

        post(new Route("/article/update/:id") {
            @Override
            public Object handle(Request request, Response response) {
                String title = request.queryParams("article-title");
                String summary = request.queryParams("article-summary");
                String content = request.queryParams("article-content");
                Integer id = Integer.parseInt(request.queryParams("article-id"));

                ArrayList<Article> articles = articleDBservice.readAll();

                for(Article article : articles) {
                    if(id.equals(article.getId())) {
                        article.setTitle(title);
                        article.setContent(content);
                        article.setSummary(summary);
                    }
                }

                response.status(200);
                response.redirect("/");
                return "";
            }
        });

        get(new Route("/article/delete/:id") {
            @Override
            public Object handle(Request request, Response response) {
                Integer id = Integer.parseInt(request.params(":id"));

                ArrayList<Article> articles = articleDBservice.readAll();

                for(Article article : articles) {
                    if(id.equals(article.getId())) {
                        article.delete();
                    }
                }

                response.status(200);
                response.redirect("/");
                return "";
            }
        });

        get(new Route("/") {
            @Override
            public Object handle(Request request, Response response) {
                String title = "My Blog";
                String createArticleLink = "<a href='/article/create'>Write Article</a>";
                StringBuilder html = new StringBuilder();
                html.append("<h1>").append(title).append("</h1>").append(createArticleLink);
                html.append("<hr>");

                ArrayList<Article> articles = articleDBservice.readAll();

                if(articles.isEmpty()) {
                    html.append("<b>No articles have been posted</b>");
                } else {
                    for(Article article : articles) {
                        if(article.readable()) {
                            html.append("Title: ").append(article.getTitle())
                                    .append("<br/>")
                                    .append(article.getCreatedAt())
                                    .append("<br/>")
                                    .append("Summary: ").append(article.getSummaryLink())
                                    .append("<br/>")
                                    .append(article.getEditLink()).append(" | ").append(article.getDeleteLink())
                                    .append("</p>");
                        }
                    }
                }
                return html.toString();
            }
        });
    }

}
