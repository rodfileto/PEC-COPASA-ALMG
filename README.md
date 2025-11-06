# PEC COPASA ALMG - Quarto Website

This is a Quarto website project.

## Prerequisites

- [Quarto](https://quarto.org/docs/get-started/) installed on your system

## Building the Website

To preview your website locally:

```bash
quarto preview
```

To render/build your website:

```bash
quarto render
```

The rendered website will be in the `_site` directory.

## Project Structure

- `_quarto.yml` - Main configuration file
- `index.qmd` - Homepage
- `about.qmd` - About page
- `styles.css` - Custom CSS styles
- `_site/` - Output directory (generated after rendering)

## Adding New Pages

1. Create a new `.qmd` file in the project root
2. Add it to the navbar in `_quarto.yml`
3. Render the site with `quarto render`

## Publishing

You can publish your website to various platforms:

- **Quarto Pub**: `quarto publish quarto-pub`
- **GitHub Pages**: `quarto publish gh-pages`
- **Netlify**: `quarto publish netlify`

For more information, visit the [Quarto documentation](https://quarto.org/docs/websites/).
