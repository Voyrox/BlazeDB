document.addEventListener("DOMContentLoaded", function() {
    removeCredits();
});

function removeCredits(){
    document.querySelectorAll('.md-copyright')[0].getElementsByTagName("a")[0].remove();
    document.querySelectorAll('.md-copyright')[0].childNodes[1].remove();
}