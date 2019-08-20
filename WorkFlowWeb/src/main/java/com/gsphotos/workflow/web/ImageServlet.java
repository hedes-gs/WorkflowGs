package com.gsphotos.workflow.web;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;

import com.gsphotos.workflow.dao.ImageFilterDAO;
import com.workflow.model.HbaseImageThumbnail;

@WebServlet("/image/*")
public class ImageServlet extends HttpServlet {

	protected List<HbaseImageThumbnail> listOfHbaseImageThumbnail;

	@Autowired
	protected ImageFilterDAO imgFilterDAO;

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String thumbId = request.getParameter("thumbId");
		int width = Integer.parseInt(request.getParameter("width"));
		int height = Integer.parseInt(request.getParameter("height"));
		boolean original = Boolean.parseBoolean(request.getParameter("original"));

		listOfHbaseImageThumbnail = imgFilterDAO.getThumbNailsByDate(LocalDateTime.now().minusDays(50),
				LocalDateTime.now().plusDays(2), 0, 0, HbaseImageThumbnail.class);
		int k = 0;
		for (HbaseImageThumbnail hit : listOfHbaseImageThumbnail) {
			if (hit.getImageId().equals(thumbId) && hit.getWidth() == width && hit.getHeight() == height
					&& hit.isOrignal() == original) {
				response.reset();
				response.setContentType("image/jpg");
				response.setContentLength(hit.getThumbnail().length);
				response.setHeader("Content-Length", String.valueOf(hit.getThumbnail().length));
				response.getOutputStream().write(hit.getThumbnail());
				response.setStatus(200);
				break;
			}
		}
	}

}